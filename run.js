const argv = require('yargs').argv;
var fs = require('fs-extra');
var path = require('path');

const BucketToBigQuery = require("./BucketToBigQuery");
const GetStorageToBuffer = require('./commands/GetStorageToBuffer');
const {Storage} = require('@google-cloud/storage');
const {ErrorControl,HttpErrors} = require('error-control');
const _ = require('lodash');

const storage = new Storage(); //{projectId: this.project, keyFilename: this.keyFilename});


async function runManifest(manifest,aStoragePath,options={}) {

  if (!manifest)
    throw new HttpErrors.NotFoundError('Manifest not found');

  let auth = manifest.authentication;
  let uri = auth.keyFilename;
  if (uri && _.startsWith(uri, 'gs://')) {
    let credentials = JSON.parse(await GetStorageToBuffer.call(storage, uri));
    auth.credentials = credentials;
    delete auth.keyFilename;
  }
  if (Object.keys(manifest.authentication).length == 0)
    delete manifest.authentication;

  let bucketToBigQuery = new BucketToBigQuery(manifest);

  // get client_email from storage
  let client_email = _.get(bucketToBigQuery, 'storage.authClient.jsonContent.client_email');
  if (!client_email) {
    let creds = await bucketToBigQuery.storage.authClient.getCredentials();
    client_email = creds.client_email;
  }
  console.log(`Authenticating as ${client_email}`);

  // let events;
  // if (context.mockEvents) {
  //   events = context.mockEvents;
  // } else {
  //   await bucketToBigQuery.ensureSubscription();
  //   events = await bucketToBigQuery.pullMessages(process.env.DEBUG ? 3 : 1000); // the more the better for deduping reasons, but we can only load 1,000 jobs of 10,000 files per job
  // }
  // console.log(`Got ${events.length} notifications`);
  // let taskInfos = bucketToBigQuery.getTriggeredTaskInfos(events);  // gets info of the tasks that have been triggered by a changing file

  let files = await bucketToBigQuery.listBucketFileUris(aStoragePath);
  let taskInfos = bucketToBigQuery.taskInfosForUris(files);

  await bucketToBigQuery.launchLoadJobsFromTaskInfos(taskInfos, options);
}


(async function() {
  try {

    const manifest_path = argv._[0];
    if (!manifest_path)
      throw new Error('Must give manifest as the first argument');
    const manifest = JSON.parse(fs.readFileSync(manifest_path));

    const bucketUri = argv._[1];

    await runManifest(manifest,bucketUri,{dryRun: !!process.env.DRY_RUN});

  } catch(e) {
    console.log(e);
  } finally {
    process.exit();
  }
})();
