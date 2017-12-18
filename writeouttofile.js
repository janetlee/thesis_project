const fs = require('fs');
const json2csv = require('./node_modules/json2csv');

var dataFaker = function(n) {
  let outfile = fs.createWriteStream('./test.csv');

  for (var i = 1 ; i <= n; i++) {
    var videoId = i % 1000;
    var viewInstanceId = i;
    var event_actions = [{action: 'PLAY', factor: -100000},
      {action: 'PAUSE', factor: -80000},
      {action: 'PLAY', factor: -70000},
      {action: 'END', factor: -1000}];

    event_actions.map(element => {
      var event_timestamp = (new Date).getTime() + element.factor;
      var action = element.action;
      var fields = [viewInstanceId, event_timestamp, action, videoId].map(el => {
        if (typeof el === 'number') {
          return el.toString();
        } else {
          return el;
        }
      });
      // console.log(fields);

      const file = json2csv({fields}) + '\n';
      console.log('Print file: ', file);
      outfile.write(file);
    });
  }
  outfile.end();
}

dataFaker(2500000);

// cqlsh command to run the file:
// COPY Events.event from '~/hack_reactor/thesis_project/test.csv' ;