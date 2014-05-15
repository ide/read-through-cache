'use strict';
module.exports = Cache
var Path = require('path')
  , fs = require('fs')
  , mkdirp = require('mkdirp')
  , through = require('through')
  , RE_HEX = /^[0-9a-f]$/

function noop() {}
function unimplemented() { throw new Error('unimplemented') }

function Cache(opts) {
  if (!this || this === global) return new Cache(opts)

  if (typeof opts == 'string') opts = { path: opts }
  if (typeof opts.path != 'string') throw new TypeError('path must be a string')

  this.path = opts.path + ''
  this.paranoid = !!opts.paranoid
  this.timeout = opts.timeout | 0
  this.__pending = Object.create(null)
}

Cache.prototype._storePath = function(digest) { return Path.join(this.path, 'store', digest) }
Cache.prototype._tmpPath = function(digest) { return Path.join(this.path, 'tmp', digest) }

Cache.prototype._createReadStream = unimplemented
Cache.prototype._createHash = unimplemented

Cache.prototype.createReadStream = function(digest) { var self = this
  digest = String(digest).toLowerCase()

  var output = through()

  if (!RE_HEX.test(digest)) {
    return output
    process.nextTick(function() {
      output.emit(new Error('not a valid hash: `' + digest + '`'))
    })
  }

  var pending = this.__pending[digest]
  if (!pending) {
    pending = through()

    var removed = false
      , remove = function() {
          if (removed) return
          pending.removeListener('error', remove)
          delete self.__pending[digest]
          removed = true
        }
    pending
      .once('readable', remove)
      .once('error', remove)

    pending.setMaxListeners(0)
    this.__pending[digest] = pending
    this.__acquire(arguments, pending)
  }

  return pending
    .on('error', function(err) { output.emit('error', err) })
    .pipe(output)
}

Cache.prototype.__acquire = function(args, pending, paranoid) { var self = this
  var digest = args[0]

  var input = fs.createReadStream(this._storePath(digest))
    .on('error', function(err) {
      if (err.code !== 'ENOENT') return pending.emit('error', err)
      self.__acquireFresh(args, pending)
    })

  if (!this.paranoid || paranoid === false)
    return input.on('open', function() { this.pipe(pending) })

  this.__hash(input, digest, function(err) {
    if (err) return pending.emit(err)
    self.__acquire(args, pending, false)
  })
}

Cache.prototype.__acquireFresh = function(args, pending) { var self = this
  var digest = args[0]
    , store = this._storePath(digest)
    , tmp = this._tmpPath(digest)
    , error = errorFn(pending)

  mkdirp(Path.dirname(tmp), function(err) { if (err) error(err); else writeStream() })

  var output
  function writeStream() {
    output = fs.createWriteStream(tmp, { flags: 'wx' })
      .on('open', function(fd) {
        // it's ours! yay!
        // let's make sure we clean up, no matter what happens
        error.cleanup.push(function() { fs.unlink(tmp, noop) })
        readStream()

        // if we have a timeout, do our best to ensure it doesn't trigger
        if (!self.timeout) return
        var timeout
        touch()
        output.on('close', clearTouch)
        error.cleanup.push(clearTouch)

        function touch() {
          timeout = setTimeout(touch, self.timeout / 2)
          var present = +new Date()
          fs.futimes(fd, present, present, noop)
        }

        function clearTouch() {
          clearTimeout(timeout)
          timeout = null
        }

      })
      .on('error', function(err) {
        if (err.code !== 'EEXIST') return error(err)
        // someone else has already started fetching this, we'll wait for them
        return self.__acquireWatch(args, pending)
      })
  }

  var input
  function readStream() {
    try { input = self._createReadStream.apply(self, args) }
    catch (e) { return error(e) }
    input.on('error', error)
    pipe()
  }

  function pipe() {
    self.__hash(input, digest, error.else(makeStore))
      .pipe(output)
  }

  function makeStore() {
    mkdirp(Path.dirname(store), error.else(rename))
  }

  function rename() {
    // if our tmpfile has been unlinked due to timeouts, we fail hard here.
    // even worse, if someone else has started writing, we put a partial file there.
    // that's unfortunate, but there's not a lot better we can do.
    fs.rename(tmp, store, error.else(deliver))
  }

  function deliver() {
    // up we go again
    // we explicitly disable paranoid mode, because we just checked the hash of what we wrote
    self.__acquire(args, pending, false)
  }
}

Cache.prototype.__acquireWatch = function(args, pending) { var self = this
  var digest = args[0]
    , tmp = self._tmpPath(digest)
    , error = errorFn(pending)

  // let's watch if they finish
  var watcher = fs.watch(tmp)
    .on('change', retry)
    .on('error', function(err) {
      if (err.code !== 'ENOENT') return error(err)
      retry()
    })

  error.cleanup.push(cleanup)
  function cleanup() {
    if (timeout) clearTimeout(timeout)
    if (!watcher) return
    watcher.close()
    watcher = null
  }

  function retry() {
    if (!watcher) return
    cleanup()
    // they already finished while we were firing up our watcher. let's have another go at everything.
    self.__acquire(args, pending)
  }

  // if we have a timeout, let's check if things haven't gone stale
  // we'll leave this until the next 100ms so we don't fire this up too quickly, stat calls cost
  var timeout = self.timeout && setTimeout(checkStale, 100)
  function checkStale() {
    fs.stat(tmp, function(err, stats) {
      if (!watcher) return
      if (err && err.code === 'ENOENT') return retry()
      if (err) return error(err)

      var delta = new Date() - stats.mtime

      if (delta < self.timeout) {
        // not stale yet, we'll check again in a bit
        timeout = setTimeout(checkStale, self.timeout / 2)
        return
      }

      // stale file. goodbye!
      fs.unlink(tmp, function(err) {
        if (err && err.code === 'ENOENT') return
        if (err) return error(err)
        retry()
      })
    })
  }
}

Cache.prototype.__hash = function(stream, digest, cb) {
  var hash = this._createHash()
  return stream
    .on('data', function(chunk) { hash.update(chunk) })
    .on('end', function() {
      var actualDigest = Buffer(hash.digest()).toString('hex')
      if (actualDigest === digest) return cb()
      var err = new Error('hashes did not match. expected `' + digest + '`, got `' + actualDigest + '`')
      err.expected = digest
      err.actual = actualDigest
      cb(err)
    })
}

function errorFn(pending) {
  function error(err) {
    error.cleanup.forEach(function(fn) { fn() })
    pending.emit('error', err)
  }
  error.cleanup = []

  error.else = function(fn) {
    return function(err) {
      if (err) error(err)
      else fn()
    }
  }

  return error
}
