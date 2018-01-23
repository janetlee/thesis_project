var cassandra = require('./node_modules/cassandra-driver');
var client = new cassandra.Client({ contactPoints: ['172.31.10.236'], keyspace: 'events' });
var QUEUE_URL = 'https://sqs.us-east-2.amazonaws.com/331983685977/packaged-events';
var AWS = require('aws-sdk');
var sqs = new AWS.SQS({region : 'us-east-2'});

module.exports.writer = function(eventRecord) {
  var query = 'INSERT INTO Events.event (videoId, viewInstanceId, event_action, event_timestamp) VALUES (?, ?, ?, ?)';
  var params = [eventRecord.videoId, eventRecord.viewInstanceId, eventRecord.eventAction, eventRecord.event_timestamp];

  client.execute(query, params, { prepare: true })
    .then(function(data) {console.log('Inserted params: ', params)})
    .catch(function(err) { console.log('Database not connected:\n', err)});
};

module.exports.reader = function(eventRecord) {
  if (eventRecord.event_action === 'END') {
    var query = 'SELECT * FROM Events.event WHERE videoid= ? AND viewinstanceid = ? allow filtering;';
    var queryParams = [eventRecord.videoId, eventRecord.viewInstanceId];

    client.execute(query, queryParams, { prepare: true })
      .then(data => {
        let cleanedRows = Array.from(data.rows);
        let events = cleanedRows.map(element => {
          return {
            "videoId": JSON.parse(element.videoid),
            "viewInstanceId": JSON.parse(element.viewinstanceid),
            "event_action": element.event_action,
            "event_timestamp": element.event_timestamp
          };
        });

        let packagedEvents = {"Events": events};
        let sqsParams = {
          MessageBody: JSON.stringify(packagedEvents),
          QueueUrl: QUEUE_URL
        };

        console.log(sqsParams);

        sqs.sendMessage(sqsParams, function(err, data) {
          console.log('RETURNING FROM THE QUEUE');
          if (err) { console.log('SQS Posting Error: ', err);  res.send("Error");}
          else { console.log('SEND QUEUE MESSAGE', sqsParams, data);  res.send("POSTED");}
        })
      })
      .catch(function(err) { console.log('Cassandra database error:\n', err)});
  } else {
    console.log('Not an END event', eventRecord);
  }
};