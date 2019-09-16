//const MemoryStream = require("./utils/MemoryStream");

const {Storage} = require('@google-cloud/storage');
const PubSubPkg = require('@google-cloud/pubsub');
const {BigQuery} = require('@google-cloud/bigquery');
const PubSub = PubSubPkg.PubSub;
const minimatch = require("minimatch");
const _ = require('lodash');
const {DateTime} = require('luxon');
const {ErrorControl,HttpErrors} = require('error-control');

const GetStorageToBuffer = require('./commands/GetStorageToBuffer');

// const stream = require('stream');
// //var util = require('util');
// // use Node.js Writable, otherwise load polyfill
// const Writable = stream.Writable; // || require('readable-stream').Writable;

// var memStore = { };
//
// /* Writable memory stream */
// function WMStrm(key, options) {
//   // allow use without new operator
//   if (!(this instanceof WMStrm)) {
//     return new WMStrm(key, options);
//   }
//   Writable.call(this, options); // init super
//   this.key = key; // save key
//   memStore[key] = new Buffer(''); // empty
// }
// util.inherits(WMStrm, Writable);

// WMStrm.prototype._write = function (chunk, enc, cb) {
//   // our memory store stores things in buffers
//   var buffer = (Buffer.isBuffer(chunk)) ?
//     chunk :  // already is Buffer use it
//     new Buffer(chunk, enc);  // string, convert
//
//   // concat to the buffer already there
//   memStore[this.key] = Buffer.concat([memStore[this.key], buffer]);
//   cb();
// };


// Trying our stream out
// var wstream = new WMStrm('foo');
// wstream.on('finish', function () {
//   console.log('finished writing');
//   console.log('value is:', memStore.foo.toString());
// });
// wstream.write('hello ');
// wstream.write('world');
// wstream.end();


function sameOrSuperSet(aListA, aListB) {
  if (_.isEqual(aListA,aListB))
    return true;
  let longest,shortest;
  if (aListA.length>aListB.length) {
    longest = aListA;
    shortest = aListB;
  } else {
    longest = aListB;
    shortest = aListA;
  }
  let start = _.slice(longest,0,shortest.length);
  return _.isEqual(start, shortest);
}

function likelyTimestamp(aName) {
  let name = _(aName).lowerCase().replace(/[\-_ ]/,'').value();
  return (name=='timestamp' || name=='createdat' || name=='time' || name=='date' || name=='datetime');
}

//const GS_URL_REGEXP = /^gs:\/\/([a-z0-9_.-]+)\/(.+)$/;

/*

Setup :

* Create bucket
* Create watch-bucket topic
* Set bucket notifications for the topic eg. : GOOGLE_APPLICATION_CREDENTIALS=/Users/gary/.credentials/bucket-to-bigquery-5de2d6358969.json gsutil notification create -t projects/bucket-to-bigquery/topics/watch-bucket -f json gs://bucket-to-bigquery-test-1
* Create bucket-to-bigquery-trigger topic
* Set up function to listen to bucket-to-bigquery-trigger topic
* Create schedule to fire bucket-to-bigquery-trigger topic

 */

class BucketToBigQuery {

  constructor(aManifest) {
    this.manifest = aManifest;
    let authentication = _.pick(aManifest.authentication,'keyFilename,credentials'.split(','));   // may contain keyFilename or credentials
    this.project = aManifest.project;
    //this.keyFilename = aManifest.authentication.keyFilename;
    this.storage = new Storage(Object.assign({projectId: this.project},authentication));
    this.pubsub = new PubSub(Object.assign({projectId: this.project},authentication));
    this.bigQuery = new BigQuery(Object.assign({projectId: this.project},authentication));
    this.subscriberClient = new PubSubPkg.v1.SubscriberClient(Object.assign({projectId: this.project},authentication));
    this.topicName = aManifest.bucketNotificationTopic;
    this.topic = this.pubsub.topic(this.topicName);
    this.subscriptionName = `${this.topicName}__BucketToBigQuery`;
    this.formattedSubscription = this.subscriberClient.subscriptionPath(this.project,this.subscriptionName);
  }

  async ensureTopicExists() {
    let [exists] = await this.topic.exists();
    if (!exists) {
      await this.topic.create();
    }
  }

  // Not working at present.
  // This works :
  // GOOGLE_APPLICATION_CREDENTIALS=/Users/gary/.credentials/bucket-to-bigquery-5de2d6358969.json gsutil notification create -t projects/bucket-to-bigquery/topics/watch-bucket -f json gs://bucket-to-bigquery-test-1
  // probably due to https://github.com/googleapis/google-cloud-php/issues/1079
  async ensureNotification(aBucket) {
    let bucket = await this.storage.bucket(aBucket);
    if (!await bucket.exists())
      throw new Error('Bucket must exist');
    //let notifications = await bucket.getNotifications(this.project);
    await this.ensureTopicExists();
    let [notification, response] = await bucket.createNotification(this.topicName, {
      customAttributes: {kind: 'bucket_notification'},
      eventTypes: ['OBJECT_FINALIZE'],
      //objectNamePrefix: ''
    });
    return notification;
  }

  //const name = `projects/${this.credentials.projectId}/subscriptions/${aSimpleName}`;
  async ensureSubscription() {
    if (!this.subscription)
      this.subscription = this.topic.subscription(this.subscriptionName);
    if ((await this.subscription.exists())[0])
      return this.subscription;
    this.subscription = await this.topic.createSubscription(this.subscriptionName, {ackDeadlineSeconds: 600}).then(r => r[0]);
    return this.subscription;
  }

  async removeSubscription() {
    this.subscription = null;
    return this.topic.subscription(this.subscriptionName).delete();
  }

  async setupBucket(aBucket) {
    let notification = await this.ensureNotification(aBucket);
    return !!notification;
  }

  async pullMessages(aMaxMessages = 10) {
    const responses = await new Promise( (resolve, reject)=> {
      this.subscriberClient.pull({
        subscription: this.formattedSubscription,
        maxMessages: aMaxMessages,
        returnImmediately: true
      }).then(resolve).catch(reject);
    });
    //let message = responses[0].receivedMessages[0];

    // {
    //   "ackId": "BCEhPjA-RVNEUAYWLF1GSFE3GQhoUQ5PXiM_NSAoRRIHAU8CKF15MlUxQVoaB1ENGXJ8ZyY8DBBUAk1RfFVbEQ16bVxXOFYPHnN6aHdvXhUFA0FXfnfQ2sugjM7gNksxIfuhkK5fev2yv6ZiZho9XhJLLD5-LzlFQV5AEkwkDERJUytDCypYEU4",
    //   "message": {
    //     "attributes": {
    //       "objectGeneration": "1567650351027743",
    //       "eventTime": "2019-09-05T02:25:51.027455Z",
    //       "eventType": "OBJECT_FINALIZE",
    //       "payloadFormat": "JSON_API_V1",
    //       "notificationConfig": "projects/_/buckets/bucket-to-bigquery-test-1/notificationConfigs/3",
    //       "bucketId": "bucket-to-bigquery-test-1",
    //       "objectId": "water/2019/09/2019-08-01_water_Lot3.csv"
    //     },
    //     "data": {
    //       "type": "Buffer",
    //       "data": [
    //         123,
    //         10,
    //         32,
    //         32,
    //         10
    //       ]
    //     },
    //     "messageId": "727179254641440",
    //     "publishTime": {
    //       "seconds": "1567650351",
    //       "nanos": 228000000
    //     },
    //     "orderingKey": ""
    //   }
    // }
    return _(responses).compact().map(r=>r.receivedMessages).flatten().value();
  }

  // getBucketFile(aUri) {
  //   const parsed = GS_URL_REGEXP.exec(aUri);
  //   if (!(parsed !== null && parsed.length === 3))
  //     throw new Error('Invalid Uri. Should be gs://<bucket>/<file>');
  //   return this.storage.bucket(parsed[1]).file(parsed[2]);
  // }

  async getLines(aUri,aMaxBytes=4000) {
    let content = await GetStorageToBuffer.call(this.storage,aUri,aMaxBytes);
    if (!content)
      return null;
    let lines = content.toString().split(/(?:\r\n|\r|\n)/g);
    return lines;
    // let file = this.getBucketFile(aUri);
    // let exists = await file.exists();
    // if (!exists[0])
    //   return null;
    // let memoryStream = new MemoryStream();
    // return new Promise((resolve, reject) => {
    //   try {
    //     file.createReadStream({
    //       start: 0,
    //       end: aMaxBytes
    //     })
    //       .pipe(memoryStream)
    //       .on('error', reject)
    //       .on('close', () => console.log('close'))
    //       .on('response', (response)=>{
    //         console.log('response');
    //       })
    //       .on('finish', () => {
    //         let s = memoryStream.buffer.toString();
    //         let lines = s.split(/(?:\r\n|\r|\n)/g);
    //         resolve(lines);
    //       })
    //       .on('finish',(e)=>console.log('finish'))
    //       .on('unpipe',(e)=>console.log('unpipe'));
    //   } catch(e) {
    //     reject(e);
    //   }
    // });
  }

  async sniffCsvHeaders(aUri) {
    let lines = await this.getLines(aUri);
    if (!lines)
      return null;
    let firstLine = lines[0];
    if (firstLine)
      return null;
    let headers = _.map(firstLine.split(','), h => _.replace(h, /^"|"$/g, ''));
    return headers;
  }

  // async pullStreaming() {
  //   const pullTimeoutms = 5000;
  //   const formattedSubscription = client.subscriptionPath(this.project,this.subscriptionName);
  //   const client = new pubsub.v1.SubscriberClient({
  //     projectId: this.project,
  //     keyFilename: this.keyFilename
  //   });
  //   const deadline = Date.now() + pullTimeoutms;
  //   const stream = client.streamingPull({subscription: formattedSubscription, deadline}).on('data', response => {
  //     // doThingsWith(response)
  //   });
  //   const streamAckDeadlineSeconds = 0;
  //   const request = {
  //     subscription: formattedSubscription,
  //     streamAckDeadlineSeconds: streamAckDeadlineSeconds,
  //   };
  //   stream.write(request);
  // }

  // async withMessages(aMaxMessages,aMessageHandler) {
  //
  //     const newAckDeadlineSeconds = 30;
  //     const request = {
  //       subscription: this.subscriptionName,
  //       maxMessages: aMaxMessages,
  //     };
  //     //try {
  //       let keepLooping = true;
  //       while (keepLooping) {
  //         let message;
  //         try {
  //           const responses = await new Promise(function (resolve, reject) {
  //             this.pubsub.pull(request).then(resolve).catch(reject);
  //           });
  //           message = responses[0].receivedMessages[0];
  //         } catch (e) {
  //           if (e.code==4) {
  //             // DEADLINE_EXCEEDED - ignore this
  //             await new Promise((resolve, reject) => {
  //               let wait = setTimeout(() => {
  //                 clearTimeout(wait);
  //                 resolve();
  //               }, 200);
  //             })
  //           } else {
  //             console.log(e);
  //           }
  //         }
  //         // if (message) {
  //         //   const ackRequest = {
  //         //     subscription: subs.name,
  //         //     ackIds: [message.ackId],
  //         //   };
  //         //   //..acknowledges the message.
  //         //   await client.acknowledge(ackRequest);
  //         // }
  //         aMessageHandler(message);
  //         keepLooping = !message; // || aMessageHandler(message) !== false;
  //       }
  //     // } finally {
  //     //   console.log('removeSubscription');
  //     //   await this.removeSubscription(aTopic,subscriptionName);
  //     //}
  //   //}
  // }

  getTriggeredTaskInfos(events) {
    let taskInfos = [];
    let tasks = this.manifest.tasks;

    if (events && events.length) {
      let jobId = `${this.manifest.jobIdPrefix}__${Math.random().toString().replace('0.','')}__${DateTime.utc().toFormat("yyyyMMdd'T'hhmmssSSS")}__`;
      for (let i = 0; i < tasks.length; i++) {
        let task = tasks[i];
        let taskInfo = {
          task,
          jobId: jobId+i.toString(),
          files: []
        };
        for (let event of events) {
          //{
          //   "kind": "storage#object",
          //   "id": "bucket-to-bigquery-test-1/water/2019/09/2019-08-01_water_Lot3.csv/1567650351027743",
          //   "selfLink": "https://www.googleapis.com/storage/v1/b/bucket-to-bigquery-test-1/o/water%2F2019%2F09%2F2019-08-01_water_Lot3.csv",
          //   "name": "water/2019/09/2019-08-01_water_Lot3.csv",
          //   "bucket": "bucket-to-bigquery-test-1",
          //   "generation": "1567650351027743",
          //   "metageneration": "1",
          //   "contentType": "application/octet-stream",
          //   "timeCreated": "2019-09-05T02:25:51.027Z",
          //   "updated": "2019-09-05T02:25:51.027Z",
          //   "storageClass": "STANDARD",
          //   "timeStorageClassUpdated": "2019-09-05T02:25:51.027Z",
          //   "size": "3475",
          //   "md5Hash": "t2LC+s4EQP4u5VfRc2kAmw==",
          //   "mediaLink": "https://www.googleapis.com/download/storage/v1/b/bucket-to-bigquery-test-1/o/water%2F2019%2F09%2F2019-08-01_water_Lot3.csv?generation=1567650351027743&alt=media",
          //   "crc32c": "KIM0Hg==",
          //   "etag": "CJ+suNLQuOQCEAE="
          // }
          let payload = (event.message.data && JSON.parse(Buffer.from(event.message.data, 'base64'))) || null;
          if (!payload || payload.kind!="storage#object")
            continue;
          let {bucket,name} = payload;
          let uri = `gs://${bucket}/${name}`;
          let isMatch = _.some(task.sources, s => minimatch(uri, s));
          console.log(`Comparing ${uri}${isMatch && '         MATCH'}`)
          if (!isMatch)
            continue;
          taskInfo.files.push(uri);
          break;
        }
        taskInfos.push(taskInfo);
      }
    }
    return taskInfos;
  }

  async loadJobsFromTaskInfos(aTaskInfos) {
    let result = [];
    for (let taskInfo of aTaskInfos) {   // each taskInfo should become a load job
      if (!taskInfo.files || !taskInfo.files.length)
        continue;
      let schema;
      let timePartitioningField;


      let firstFileHeaders = null;
      for (let f of taskInfo.files) {
        try {
          if (firstFileHeaders = await this.sniffCsvHeaders(f))
            break;
        } catch (e) {
          console.log(e);
          throw e;
        }
      }
      if (!firstFileHeaders)
        continue;
      let lastFileHeaders = null;
      if (taskInfo.files.length > 1) {
        for (let i = taskInfo.files.length - 1; i >= 0; i--) {
          try {
            if (lastFileHeaders = await this.sniffCsvHeaders(taskInfo.files[i]))
              break;
          } catch (e) {
            console.log(e);
            throw e;
          }
        }
      }
      let longestHeaders = !lastFileHeaders || (firstFileHeaders.length>=lastFileHeaders.length) ? firstFileHeaders : lastFileHeaders;

      let table = this.bigQuery.dataset(taskInfo.task.dataset).table(taskInfo.task.table);
      let tableExists = (await table.exists())[0];
      if (tableExists) {
        let metadata = await table.getMetadata();
        schema = metadata[0].schema;
        let columns = schema.fields;
        let columnNames = _.map(columns,c=>c.name);
        if (!sameOrSuperSet(longestHeaders,columnNames))
          throw new Error('The incoming fields are not a superset of the table fields');
        let isExpanding = longestHeaders.length > columnNames.length;
        if (isExpanding) {
          schema = _.cloneDeep(schema);
          for (let i=columnNames.length;i<longestHeaders.length;i++) {
            let name = longestHeaders[i];
            schema.fields.push({name,type: 'float'});
          }
        }
      } else {
        let fields;
        if (taskInfo.task.fields && taskInfo.task.fields.length) {
          if (!sameOrSuperSet(longestHeaders,_.map(taskInfo.task.fields,f=>f.name)))
            throw new Error('The incoming fields are not a superset of the manifest fields');
          fields = taskInfo.task.fields;
        } else {
          fields = [];
        }
        for (let i=fields.length;i<longestHeaders.length;i++) {
          let name = longestHeaders[i];
          fields.push({name,type: 'float'});
        }
        if (taskInfo.task.timePartitioningField) {
          timePartitioningField = taskInfo.task.timePartitioningField;
        } else if (taskInfo.task.timePartitioningField===undefined) {
          timePartitioningField = _.find(_.slice(fields,0,5),likelyTimestamp);
        } else
          timePartitioningField = null;
        schema = {fields};
      }

      let jobValues = {
        jobId: taskInfo.jobId,
        location: 'US',
        configuration: {
          load: {
            sourceFormat: 'CSV',
            skipLeadingRows: 1,
            allowJaggedRows: true,
            allowQuotedNewlines: true,
            ignoreUnknownValues: true,
            maxBadRecords: 0,
            sourceUris: taskInfo.files,
            destinationTable: {
              projectId: this.project,
              datasetId: taskInfo.task.dataset,
              tableId: taskInfo.task.table
            },
            schema,
          }
        }
      };
      if (timePartitioningField)
        jobValues.configuration.load.timePartitioning = {
          type: 'DAY',
          field: timePartitioningField
        };
      result.push(jobValues);
    }
    return result;
  }

  // async
  launchLoadJobs(loadJobs) {
    return Promise.all(_.map(loadJobs,j=>this.bigQuery.createJob(j)));
  }

  ackMessages(aMessages) {
    const ackIds = _.map(aMessages,m=>m.ackId);
    return this.subscriberClient.acknowledge({
      subscription: this.formattedSubscription,
      ackIds: ackIds,
    });
  }

};

module.exports = BucketToBigQuery;
