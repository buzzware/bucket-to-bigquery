const stream = require('stream');
// use Node.js Writable, otherwise load polyfill
const Writable = stream.Writable; // || require('readable-stream').Writable;

class MemoryStream extends Writable {

  constructor() {
    super();
    this._memstore = new Buffer('');
  }

  get buffer() {
    return this._memstore;
  }

  _write(chunk, enc, cb) {
    // our memory store stores things in buffers
    var buffer = (Buffer.isBuffer(chunk)) ?
      chunk :  // already is Buffer use it
      new Buffer(chunk, enc);  // string, convert

    // concat to the buffer already there
    this._memstore = Buffer.concat([this._memstore, buffer]);
    cb();
  }
}

module.exports = MemoryStream;
