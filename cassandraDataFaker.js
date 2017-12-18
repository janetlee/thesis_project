const cassandra = require('cassandra-driver');
const client = new cassandra.Client({ contactPoints: ['localhost'], keyspace: 'events' });
const Promise = require('bluebird');

client.connect(function (err) {
  if (err) {
    console.log('Database not connected:\n', err);
  } else {
    console.log('Database connected\n');
  }
});

var dataFaker = function(n, multiplier) {
  for (var i = 1 + n * (multiplier-1); i <= n * multiplier; i++) {
    console.log('INSERT: ', i, 'Time: ', Date.now());
    var videoId = i % 1000;
    var viewInstanceId = i;
    var event_actions = [{action: 'PLAY', factor: -100000},
      {action: 'PAUSE', factor: -80000},
      {action: 'PLAY', factor: -70000},
      {action: 'END', factor: -1000}];

    var queries = event_actions.map(element => {
      var event_timestamp = (new Date).getTime() + element.factor;
      var action = element.action;

      var query = 'INSERT INTO Events.event (videoId, viewInstanceId, event_action, event_timestamp) VALUES (?, ?, ?, ?)';
      var params = [videoId, viewInstanceId, action, event_timestamp];
      console.log(params);

      return {query: query, params: params};
    });

      client.batch(queries, { prepare: true })
        .then(function(data) {console.log('Inserted')})
        .catch(function(err) { console.log('Database not connected:\n', err)});

    }
};


dataFaker(10000, 4);