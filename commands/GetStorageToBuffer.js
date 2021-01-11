const MemoryStream = require('../utils/MemoryStream');

const GS_URL_REGEXP = /^gs:\/\/([a-z0-9_.-]+)\/(.+)$/;

class GetStorageToBuffer {

  static getBucketFile(storage, aUri) {
    const parsed = GS_URL_REGEXP.exec(aUri);
    if (!(parsed !== null && parsed.length === 3))
      throw new Error('Invalid Uri. Should be gs://<bucket>/<file>');
    return storage.bucket(parsed[1]).file(parsed[2]);
  }

  static async call(storage, aUri, aMaxBytes = 1024*1024) {
    let file = this.getBucketFile(storage, aUri);
    let exists = await file.exists();
    if (!exists[0])
      return null;
    let memoryStream = new MemoryStream();
    return await new Promise((resolve, reject) => {
      try {
        let options = aMaxBytes ? {start: 0, end: aMaxBytes} : {};
        file.createReadStream(options)
          .pipe(memoryStream)
          .on('error', reject)
          // .on('close', () => console.log('close'))
          // .on('response', (response)=>{
          //   console.log('response');
          // })
          .on('finish', () => {
            resolve(memoryStream.buffer);
            // let s = memoryStream.buffer.toString();
            // let lines = s.split(/(?:\r\n|\r|\n)/g);
            // resolve(lines);
          })
        // .on('finish',(e)=>console.log('finish'))
        // .on('unpipe',(e)=>console.log('unpipe'));
      } catch (e) {
        reject(e);
      }
    });
  }
}
module.exports = GetStorageToBuffer;
