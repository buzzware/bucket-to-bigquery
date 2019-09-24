const {Storage} = require('@google-cloud/storage');
const PubSubPkg = require('@google-cloud/pubsub');
const {BigQuery} = require('@google-cloud/bigquery');
const PubSub = PubSubPkg.PubSub;
const minimatch = require("minimatch");
const _ = require('lodash');
const {DateTime} = require('luxon');
const {ErrorControl,HttpErrors} = require('error-control');

const GetStorageToBuffer = require('./commands/GetStorageToBuffer');

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

class BucketToBigQuery {

  constructor(aManifest) {
    this.manifest = aManifest;
    let authentication = _.pick(aManifest.authentication,'keyFilename,credentials'.split(','));   // may contain keyFilename or credentials
    this.project = aManifest.project;
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

  async ensureTable(aDataset,aTable,aDefinition) {
    let table = this.bigQuery.dataset(aDataset).table(aTable);
    let exists = (await table.exists())[0];
    if (!exists)
      await table.create(aDefinition);
  }

  // Not working at present.
  // This works :
  // GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json gsutil notification create -t projects/<project>/topics/watch-bucket -f json gs://<bucket>
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

  async pullMessages(aMaxMessages = 100) {
    let results = [];
    let zeros = 5;    // stop when we have received this many empty responses
    while (results.length < aMaxMessages) {
      let pullCount = aMaxMessages - results.length;
      let responses = await new Promise((resolve, reject) => {
        this.subscriberClient.pull({
          subscription: this.formattedSubscription,
          maxMessages: pullCount,
          returnImmediately: true
        }).then(resolve).catch(reject);
      });
      responses = _(responses).compact().map(r => r.receivedMessages).flatten().value();
      console.log(`Pulled ${responses.length} actual responses`);
      if (!responses.length) {
        zeros -= 1;
        if (!zeros)
          break;
      }
      results = _.concat(results,responses);
    }
    return results;
  }


  async getLines(aUri,aMaxBytes=4000) {
    let content = await GetStorageToBuffer.call(this.storage,aUri,aMaxBytes);
    if (!content)
      return null;
    let lines = content.toString().split(/(?:\r\n|\r|\n)/g);
    return lines;
  }

  async sniffCsvHeaders(lines) {
    if (!lines)
      return null;
    let firstLine = lines[0];
    if (!firstLine)
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
      // decode event.message.data
      events = _.filter(events, ['message.attributes.eventType', 'OBJECT_FINALIZE']);
      for (let event of events) {
        let data = _.get(event, 'message.data') || null;
        if (data)
          event.message.data = JSON.parse(Buffer.from(data, 'base64'));
      }
      events = _.filter(events, ['message.data.kind', 'storage#object']);
      events = _.uniqBy(events, 'message.data.selfLink');

      console.log(`Got ${events.length} unique storage finalize events`);

      let jobId = `${this.manifest.jobIdPrefix}__${Math.random().toString().replace('0.', '')}__${DateTime.utc().toFormat("yyyyMMdd'T'hhmmssSSS")}__`;
      for (let i = 0; i < tasks.length; i++) {
        let task = tasks[i];
        let taskInfo = {
          task,
          jobId: jobId + i.toString(),
          files: []
        };
        for (let event of events) {
          let data = event.message.data;
          let {bucket, name} = data;
          let uri = `gs://${bucket}/${name}`;
          let isMatch = _.some(task.sources, s => minimatch(uri, s));
          console.log(`Comparing ${uri}${isMatch && '         MATCH'}`);
          if (isMatch)
            taskInfo.files.push(uri);
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
          let lines = await this.getLines(f);
          if (firstFileHeaders = await this.sniffCsvHeaders(lines))
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
            let lines = await this.getLines(taskInfo.files[i]);
            if (lastFileHeaders = await this.sniffCsvHeaders(lines))
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
      let filesToLoad = taskInfo.files;
      let filesImported = await this.checkFilesImported(taskInfo.task.dataset,taskInfo.task.table,filesToLoad);
      console.log(`filesToLoad ${filesToLoad.length} already loaded ${filesImported.length}`);
      filesToLoad = _.difference(filesToLoad,filesImported);
      if (!filesToLoad.length)
        continue;

      let jobValues = {
        jobId: taskInfo.jobId,
        location: taskInfo.task.location || 'US',
        configuration: {
          load: {
            sourceFormat: 'CSV',
            skipLeadingRows: 1,
            allowJaggedRows: true,
            allowQuotedNewlines: true,
            ignoreUnknownValues: true,
            maxBadRecords: 0,
            sourceUris: filesToLoad,
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
    return Promise.all(_.map(loadJobs,j=> {
      return this.bigQuery.createJob(j);
    }));
  }

  async storeJobsFilesAsImported(loadJobs) {
    for (let j of loadJobs) {
      let load = _.get(j,'configuration.load');
      if (!load)
        continue;
      await this.storeAsImported(load.destinationTable.datasetId,load.destinationTable.tableId,load.sourceUris);
    }
  }

  ackMessages(aMessages) {
    const ackIds = _.map(aMessages,m=>m.ackId);
    return this.subscriberClient.acknowledge({
      subscription: this.formattedSubscription,
      ackIds: ackIds,
    });
  }

  async checkFilesImported(aDataset, aTable, aFiles) {
    let files_s = _.map(aFiles,f=>`'${f}'`).join(',');
    let results = await this.bigQuery.query(`SELECT DISTINCT uri FROM ${aDataset}.${aTable}_imported WHERE uri IN (${files_s})`);
    return _.map(results[0],'uri');
  }

  async storeAsImported(aDataset, aTable, aFiles) {
    let table = this.bigQuery.dataset(aDataset).table(`${aTable}_imported`);
    let imported_at = DateTime.utc().toFormat('yyyy-MM-dd HH:mm:ss');
    let rows = _.map(aFiles,f => ({imported_at, uri: f}));
    await table.insert(rows);
  }
};

module.exports = BucketToBigQuery;
