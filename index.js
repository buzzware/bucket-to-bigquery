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

  console.log(JSON.stringify(event));
  console.log(JSON.stringify(context));

  let manifest;
  let manifest_uri = context.env.B2BQ_MANIFEST;
  if (!manifest_uri)
    throw new HttpErrors.NotFound('B2BQ_MANIFEST not given');
  //let projectId = context.
  let manifest_content = await GetStorageToBuffer.call(storage, manifest_uri);
  manifest = manifest_content && JSON.parse(manifest_content);
  if (!manifest)
    throw new HttpErrors.NotFound('Manifest not found');

  let auth = manifest.authentication;
  let uri = auth.keyFilename;
  if (uri && _.startsWith(uri,'gs://')) {
    let credentials = JSON.parse(await GetStorageToBuffer.call(storage, uri));
    auth.credentials = credentials;
    delete auth.keyFilename;
  }

  // let payload;
  // try {
  //   payload = (event.data && JSON.parse(Buffer.from(event.data, 'base64'))) || null;
  // } catch (e) {
  //   console.error(`Failed to decode data sent to this pubsub function. You must send a string like {"data":"eyJtZXNzYWdlIjogImhlbGxvIn0"} which decodes to {"message": "hello"}`);
  //   throw e;
  // }
  // console.log("stringified:" + (payload && JSON.stringify(payload)) || 'null');

  let bucketToBigQuery = new BucketToBigQuery(manifest);
  // bucketToBigQuery.withMessages(10, (message) => {
  //   bucketToBigQuery.createLoadJobs(payload);
  // })

  let events;
  if (context.mockEvents) {
    events = context.mockEvents;
  } else {
    await bucketToBigQuery.ensureSubscription();
    events = await bucketToBigQuery.pullMessages();
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
    let responses = await bucketToBigQuery.launchLoadJobs(loadJobs);
  }
  if (events && events.length)
    await bucketToBigQuery.ackMessages(events);
  console.log('done.');
};
