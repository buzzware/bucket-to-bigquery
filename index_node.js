const index = require('./index');

(async function() {
  try {
    let storage_event = JSON.stringify({
      "kind": "storage#object",
      "id": "bucket-to-bigquery-test-1/water/2019/09/2019-08-01_water_Lot3.csv/1567650351027743",
      "selfLink": "https://www.googleapis.com/storage/v1/b/bucket-to-bigquery-test-1/o/water%2F2019%2F09%2F2019-08-01_water_Lot3.csv",
      "name": "water/2019/09/2019-08-01_water_Lot3.csv",
      "bucket": "bucket-to-bigquery-test-1",
      "generation": "1567650351027743",
      "metageneration": "1",
      "contentType": "application/octet-stream",
      "timeCreated": "2019-09-05T02:25:51.027Z",
      "updated": "2019-09-05T02:25:51.027Z",
      "storageClass": "STANDARD",
      "timeStorageClassUpdated": "2019-09-05T02:25:51.027Z",
      "size": "3475",
      "md5Hash": "t2LC+s4EQP4u5VfRc2kAmw==",
      "mediaLink": "https://www.googleapis.com/download/storage/v1/b/bucket-to-bigquery-test-1/o/water%2F2019%2F09%2F2019-08-01_water_Lot3.csv?generation=1567650351027743&alt=media",
      "crc32c": "KIM0Hg==",
      "etag": "CJ+suNLQuOQCEAE="
    });
    let event = {
      //data: Buffer.from(data).toString('base64')
    };
    let context = {
      env: {B2BQ_MANIFEST: 'gs://bucket-to-bigquery.appspot.com/manifest.json'}
    };
    context.mockEvents = [{message: {data: Buffer.from(storage_event).toString('base64')}}];
    await index.loadCreatedFiles(event,context);
  } catch(e) {
    console.log(e.message);
  } finally {
    process.exit();
  }
})();
