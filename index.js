const BucketToBigQuery = require("./BucketToBigQuery");
const GetStorageToBuffer = require('./commands/GetStorageToBuffer');
const {Storage} = require('@google-cloud/storage');
const {ErrorControl,HttpErrors} = require('error-control');
const _ = require('lodash');

const storage = new Storage(); //{projectId: this.project, keyFilename: this.keyFilename});

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event Event payload.
 * @param {!Object} context Metadata for the event.
 */
exports.loadCreatedFiles = async (event, context) => {

  let manifest;
  let manifest_uri = process.env.B2BQ_MANIFEST;
  if (!manifest_uri)
    throw new HttpErrors.NotFoundError('B2BQ_MANIFEST not given');
  //let projectId = context.
  let manifest_content = await GetStorageToBuffer.call(storage, manifest_uri);
  manifest = manifest_content && JSON.parse(manifest_content);
  if (!manifest)
    throw new HttpErrors.NotFoundError('Manifest not found');

  let auth = manifest.authentication;
  let uri = auth.keyFilename;
  if (uri && _.startsWith(uri,'gs://')) {
    let credentials = JSON.parse(await GetStorageToBuffer.call(storage, uri));
    auth.credentials = credentials;
    delete auth.keyFilename;
  }
  if (Object.keys(manifest.authentication).length==0)
    delete manifest.authentication;

  let bucketToBigQuery = new BucketToBigQuery(manifest);

  // get client_email from storage
  let client_email = _.get(bucketToBigQuery,'storage.authClient.jsonContent.client_email');
  if (!client_email) {
    let creds = await bucketToBigQuery.storage.authClient.getCredentials();
    client_email = creds.client_email;
  }

  console.log(`Authenticating as ${client_email}`);

  let events;
  if (context.mockEvents) {
    events = context.mockEvents;
  } else {
    await bucketToBigQuery.ensureSubscription();
    events = await bucketToBigQuery.pullMessages(process.env.DEBUG ? 3 : 1000); // the more the better for deduping reasons, but we can only load 1,000 jobs of 10,000 files per job
  }
  console.log(`Got ${events.length} notifications`);
  let taskInfos = bucketToBigQuery.getTriggeredTaskInfos(events);  // gets info of the tasks that have been triggered by a changing file

  await this.launchLoadJobsFromTaskInfos(taskInfos,{dryRun: !!process.env.DRY_RUN});

  if (events && events.length) {
    console.log(`ACK ${events.length} messages`);
    await bucketToBigQuery.ackMessages(events);
  }
  console.log(`done.`);
};
