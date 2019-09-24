# Bucket To BigQuery

### Support consulting is available for this project from Buzzware Solutions. Please contact us via http://buzzware.com.au

This project implements a common pattern for handling time-series data streams on Google Cloud.
This pattern was refined to achieve the following :
* CSV files can be stored cheaply, downloaded, emailed and loaded into Excel for analysis by non-programmers
* The CSV files must be compatible with BigQuery for loading via load jobs (free, as opposed to inserts) and eventual use in Data Studio
* The table schema must auto-expand to receive new columns in the CSV data

The pattern looks like this :

* Data is captured from a source, and uploaded to a bucket with a filename that includes the date stamp of the data
    * The file is in CSV format with a timestamp column, a few other optional columns indicating the source of the data, and a series of data channel columns
    * The timestamp column is in SQL format, compatible with both Excel and BigQuery
    * The timestamp is in UTC, without any timezone indicator that would make it incompatible with Excel
    * A utc_offset float column stores the UTC offset of the physical location where the data was recorded. The local time can then be calculated by adding the utc_offset to the timestamp
* Bucket Notifications are set up to create an event on a bucket watch topic in PubSub when a file is successfully uploaded
* The script is triggered periodically eg. every 24 hours to minimise the number of load jobs
* When the main script is triggered, events are pulled from the bucket watch topic, from which a list of files are loaded into BigQuery via load job(s)

This means that as long as files match the following, they will be loaded into BigQuery whenever the script is run : 
* file path patterns defined in the manifest
* columns as defined in the manifest and in the right order
* additional columns only added progressively to the right. Columns can never be removed or re-ordered

This is a npm package, and also can be zipped and uploaded as-is to create a Google Cloud Function. Configuration is provided by environment variables, in particular a manifest file stored in any bucket.

This script has several features of note that make it more useful and flexible than BigQuery Data Transfer Service (BDTS) :

* Auto-expanding schema : Columns added to the right side of your data are automatically added to the BigQuery table (currently float type is assumed)
* Better pattern matching than BDTS, especially ** so you can store your csv files in a folder heirarchy eg. by year
* Tries to be as permissive as possible with incoming data - still limited by what load jobs allow
* Maintains and checks a table of already loaded files to prevent duplicate rows in your table
* Uses a manifest file for specifying multiple tasks that each have multiple source file patterns and one destination table
* If the destination table does not exist, it will be created with the schema in the manifest

## Time Partitioning
* It is recommended to use time partitioning on your destination table. 
* This requires a column of timestamp (not datetime) format, and that means it does not contain a timezone, and so should be in UTC. 
* This approach means that BigQuery can select rows within a time range without scanning the entire table, saving cost and improving speed.
* It is also recommended that a utc_offset float column be added to store the timezone (eg. in hours). Both Excel and BigQuery can then combine the timestamp and utc_offset columns into a local_time column with the correct timezone.
* It is recommended that a view called something like <table name>_ordered be created which orders the rows by time, and replaces the UTC timestamp and utc_offset with a local_time column of datetime type with the timezone correctly set. As views are cached, and views can be based on other views, if all requests use these views, query costs, programming errors and programming time will be reduced.
* Here is example SQL for such a view :    
```sql
select DATETIME(timestamp,CONCAT(IF(utc_offset>0,'+','-'),FORMAT_TIME("%R", TIME_ADD(TIME "00:00:00", INTERVAL CAST(ABS(ROUND(utc_offset*60)) AS INT64) MINUTE)))) as local_time,* except (timestamp,utc_offset) from <projectId>.<datasetId>.<tableId> order by timestamp
```
* For Data Studio users, it is recommended that a view like the above, or a derivative view be used for all reports. The local_time column will contain the full time and timezone information from the data source, and so Data Studio is able to correctly select by time and display the time as expected by users. This avoids a common trap where time column does not have its timezone set correctly or at 
all, and eg. reports by day may include hours from previous or future days.   

## Manifest Format

```json5
{
  "project": "<your Google Cloud Project Id>",
  "bucketNotificationTopic": "<a PubSub topic name for watching source buckets eg. 'watch-bucket'>",
  "authentication": {
    // any fields here will be merged in to the options for instantiating the Google APIs (Storage, BigQuery, PubSub). Leaving this empty will mean using the default service account.
  },
  "jobIdPrefix": "<your prefix>__", // will be added as a prefix when creating job names
  "tasks": [
    {
      "sources": [  // a list of file path patterns to load into the given table
        "gs://<bucket>/<folder>/**/*.csv"
      ],
      "dataset": "<dataset>",
      "table": "<table>",
      "timePartitioningField": "timestamp",     
      "fields": [
        {
          "name": "timestamp",
          "type": "timestamp"
        },
        {
          "name": "utc_offset",
          "type": "float"
        },
        {
          "name": "location",
          "type": "string"
        }
      ]
    }
  ]
}
```


## Deployment
* Create bucket
* Create watch-bucket topic
* Set bucket notifications for the topic eg. : 
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json gsutil notification create -t projects/<project>/topics/watch-bucket -f json gs://<bucket>
```
You can check notifications using 
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json gsutil notification list gs://<bucket>
```

* Create <optional-custom-name>manifest.json file and upload to <project>.appspot.com bucket
* Create cloud function with bucket-to-bigquery repository. From bucket-to-bigquery directory: 
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json gcloud functions deploy loadCreatedFiles --project <project> --trigger-topic trigger_loadCreatedFiles --set-env-vars B2BQ_MANIFEST=gs://<bucket>/manifest.json --runtime nodejs8 --memory 128MB --entry-point loadCreatedFiles --source=.
```
* Create schedule to fire trigger_loadCreatedFiles topic with an empty payload
