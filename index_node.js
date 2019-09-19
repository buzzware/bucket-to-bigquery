const index = require('./index');

(async function() {
  try {
    let event = {
      //data: Buffer.from(data).toString('base64')
    };
    let context = {
      //env: {B2BQ_MANIFEST: 'gs://bucket-to-bigquery.appspot.com/manifest.json'}
    };
    //context.mockEvents = [{message: {data: Buffer.from(storage_event).toString('base64')}}];
    await index.loadCreatedFiles(event,context);
  } catch(e) {
    console.log(e.message);
  } finally {
    process.exit();
  }
})();
