var cassandra = require('./node_modules/cassandra-driver');
var client = new cassandra.Client({ contactPoints: ['172.31.14.84'], keyspace: 'events' });


module.exports.handler = function(eventRecord) {
  var query = 'INSERT INTO Events.event (videoId, viewInstanceId, event_action, event_timestamp) VALUES (?, ?, ?, ?)';
  var params = [eventRecord.videoId, eventRecord.viewInstanceId, eventRecord.eventAction, eventRecord.event_timestamp];

  client.execute(query, params, { prepare: true })
    .then(function(data) {console.log('Inserted params: ', params)})
    .catch(function(err) { console.log('Database not connected:\n', err)});

};