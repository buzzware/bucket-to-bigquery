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

  // console.log(JSON.stringify(process.env));
  // console.log(JSON.stringify(event));
  // console.log(JSON.stringify(context));

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

  // let payload;
  // try {
  //   payload = (event.data && JSON.parse(Buffer.from(event.data, 'base64'))) || null;
  // } catch (e) {
  //   console.error(`Failed to decode data sent to this pubsub function. You must send a string like {"data":"eyJtZXNzYWdlIjogImhlbGxvIn0"} which decodes to {"message": "hello"}`);
  //   throw e;
  // }
  // console.log("stringified:" + (payload && JSON.stringify(payload)) || 'null');

  let bucketToBigQuery = new BucketToBigQuery(manifest);

  // get client_email from storage
  let client_email = _.get(bucketToBigQuery,'storage.authClient.jsonContent.client_email');
  if (!client_email) {
    let creds = await bucketToBigQuery.storage.authClient.getCredentials();
    client_email = creds.client_email;
  }

  console.log(`Authenticating as ${client_email}`);
  // bucketToBigQuery.withMessages(10, (message) => {
  //   bucketToBigQuery.createLoadJobs(payload);
  // })

  let events;
  if (context.mockEvents) {
    events = context.mockEvents;
  } else {
    await bucketToBigQuery.ensureSubscription();
    events = await bucketToBigQuery.pullMessages(9000); // the more the better for deduping reasons, but we can only load 1,000 jobs of 10,000 files per job
  }
  console.log(`Got ${events.length} notifications`);
  let taskInfos = bucketToBigQuery.getTriggeredTaskInfos(events);  // gets info of the tasks that have been triggered by a changing file
  for (let t of taskInfos) {
    console.log(`jobId ${t.jobId}`);
    console.log(`dataset: ${t.task.dataset} table: ${t.task.table}`);
    console.log(`${t.files.length} files`);
    console.log(`first file : ${t.files[0]}`);
    if (t.files.length>1)
      console.log(`last file  : ${t.files[t.files.length-1]}`);
  }
  if (taskInfos && taskInfos.length) {
    let loadJobs = await bucketToBigQuery.loadJobsFromTaskInfos(taskInfos);
    console.log(`Generated ${loadJobs && loadJobs.length} loadJobs`);
    if (process.env.DRY_RUN) {
      console.log(JSON.stringify(loadJobs));
    } else {
      await bucketToBigQuery.launchLoadJobs(loadJobs);
      console.log('sent load jobs');
    }
  }
  if (events && events.length) {
    console.log(`ACK ${events.length} messages`);
    await bucketToBigQuery.ackMessages(events);
  }
  console.log(`done.`);
};
