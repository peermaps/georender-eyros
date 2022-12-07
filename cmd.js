#!/usr/bin/env node

var minimist = require('minimist')
var argv = minimist(process.argv.slice(2), {
  alias: {
    d: 'datadir',
    f: 'format',
    h: 'help',
  },
  default: { datadir: '.' },
})
if (argv.help) return usage()

var fs = require('fs')
var path = require('path')
var varint = require('varint')
var through = require('through2')
var split = require('split2')
var pump = require('pump')
var pumpify = require('pumpify')

var fmt = null
if (argv.format === 'hex' || argv.format === 'base64') {
  fmt = pumpify(split(), through(function (buf, enc, next) {
    next(null, Buffer.from(buf.toString(), argv.format))
  }))
} else {
  var lp = require('length-prefixed-stream')
  fmt = lp.decode()
}
var batch = []
var bsize = 10_000
var syncSize = 100*bsize
var bstream = through.obj(
  function (buf, enc, next) {
    var point = getPoint(buf)
    if (!point) return next()
    batch.push({ type: 'insert', point, value: buf })
    if (batch.length < bsize) {
      next()
    } else {
      next(null, batch)
      batch = []
    }
  },
  function (next) {
    if (batch.length > 0) {
      next(null, batch)
    } else {
      next()
    }
  }
)

fs.mkdirSync(argv.datadir, { recursive: true })
;(async function () {
  var eyros = require('eyros/2d')
  var raf = require('random-access-file')
  var db = await eyros({
    wasmSource: fs.readFileSync(require.resolve('eyros/2d.wasm')),
    storage: (name) => {
      var file = path.join(argv.datadir,name)
      var storage = raf(file)
      storage.len = function (cb) {
        fs.stat(file, function (err, s) {
          if (err && err.code === 'ENOENT') cb(null, 0)
          else if (err) cb(err)
          else cb(null, s.size)
        })
      }
      return storage
    },
  })
  var count = 0
  var ingest = through.obj(write, end)
  pump(process.stdin, fmt, bstream, ingest)
  function write(batch, enc, next) {
    count += batch.length
    db.batch(batch).then(() => {
      if (count > syncSize) {
        count = 0
        db.sync().then(next).catch(next)
      } else next()
    }).catch(next)
  }
  function end(next) {
    if (count > 0) {
      db.sync().then(next).catch(next)
    } else {
      next()
    }
  }
})()

function getPoint(buf) {
  var ft = buf[0]
  if (ft !== 1 && ft !== 1 && ft !== 2 && ft !== 3 && ft !== 4) return null
  var offset = 1
  var t = varint.decode(buf, offset)
  offset += varint.decode.bytes
  var id = varint.decode(buf, offset)
  offset += varint.decode.bytes
  if (ft === 0x01) {
    var lon = buf.readFloatLE(offset)
    offset += 4
    var lat = buf.readFloatLE(offset)
    offset += 4
    return [lon,lat]
  } else  if (ft === 0x02 || ft === 0x03 || ft === 0x04) {
    var pcount = varint.decode(buf, offset)
    offset += varint.decode.bytes
    if (pcount === 0) return null
    var point = [[Infinity,-Infinity],[Infinity,-Infinity]]
    for (var i = 0; i < pcount; i++) {
      var lon = buf.readFloatLE(offset)
      offset += 4
      var lat = buf.readFloatLE(offset)
      offset += 4
      point[0][0] = Math.min(point[0][0], lon)
      point[0][1] = Math.max(point[0][1], lon)
      point[1][0] = Math.min(point[1][0], lat)
      point[1][1] = Math.max(point[1][1], lat)
    }
    if (pcount === 1) return point[0]
    return point
  }
  return null
}

function usage() {
  console.log(`
    usage: georender-eyros [FILE] {OPTIONS}

    Write georender data from stdin or FILE into an eyros database.

      -f --format   Input georender format: hex, base64, or lp (default)
      -d --datadir  Write to eyros database in this directory.
      -h --help     Show this message.

  `.trim().replace(/^ {4}/gm,'') + '\n')
}
