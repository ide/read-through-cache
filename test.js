'use strict';
var Cache = require('./')
  , fs = require('fs')
  , mkdirp = require('mkdirp')
  , through = require('through')
  , http = require('http')
  , path = __dirname + '/test'
  , cache = Cache({ path: path })

mkdirp.sync(path + '/output')

cache._createReadStream = function(key, url) {
  var stream = through()
  http.get(url)
    .on('response', function(res) {
      if (res.statusCode === 200) return res.pipe(stream)
      var err = new Error('HTTP ' + res.statusCode)
      err.statusCode = res.statusCode
      stream.emit('error', err)
    })
    .on('error', function(err) { stream.emit('error', err) })
  return stream
}

cache
  .createReadStream('4f8edd5e8cfb55cd2755ac6505593c2b4d5510f8', 'http://registry.npmjs.org/npm/-/npm-1.4.10.tgz')
  .pipe(fs.createWriteStream(path + '/output/npm' + rand() + '.tgz'))

cache
  .createReadStream('4f8edd5e8cfb55cd2755ac6505593c2b4d5510f8', 'http://registry.npmjs.org/npm/-/npm-1.4.10.tgz')
  .pipe(fs.createWriteStream(path + '/output/npm' + rand() + '.tgz'))

// Test errors
var errorCache = Cache({ path: path })
errorCache._createReadStream = function(key) {
  var stream = through()
  process.nextTick(function() {
    stream.emit('error', new Error('Intentional error'))
  })
  return stream
}

function readErrorStream(callback) {
  var timer = setTimeout(function() {
    console.log('Read from the error stream has stalled')
  }, 100)

  errorCache
    .createReadStream('fail')
    .on('error', function(error) {
      clearTimeout(timer)
      // We actually expect an error, so pass the callback a result
      callback(null, error)
    })
    .on('end', function() {
      clearTimeout(timer)
      callback(new Error('Error stream unexpectedly ended'))
    })
}

readErrorStream(function(error, result) {
  if (error) {
    console.error('Something went wrong:', error)
  } else {
    // A subsequent read should not stall
    readErrorStream(function(error, result) {
      if (error) {
        console.error('Something went wrong:', error)
      }
    });
  }
})

function rand() {
  return (Math.random() * 0xFFFFFFFF) | 0
}
