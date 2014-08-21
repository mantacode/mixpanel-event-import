#!/usr/bin/env node

var program = require('commander');
var request = require('request');
var csv = require('fast-csv');
var fs = require('fs');
var chalk = require('chalk');
var moment = require('moment');
var _ = require('underscore');
var async = require('async');
var requestCount = 0;

program.usage('<command> options');
program.version(require('./package').version);
program.name = 'mixpanel';

program
  .command('import <file>')
  .description('Import historical events to mixpanel')
  .option('-t, --token <token>', 'Token for mixpanel project', process.env.MIXPANEL_TOKEN)
  .option('-k, --api-key <key>', 'Mixpanel API key', process.env.MIXPANEL_KEY)
  .option('--dry-run', 'Log event output rather than actually submitting them')
  .option('-l, --log', 'Log status after each event')
  .option('-d, --date-format <format>', 'Format for dates', 'MM/DD/YYYY')
  .action(function(file, options) {
    var stream = fs.createReadStream(file);
    var input = csv().on('record', function(data) {
      var obj = {
        'event': data[0],
        properties: {
          distinct_id: data[1],
          time: moment(data[2], options.dateFormat).unix(),
          token: options.token
        }
      };

      _(data.slice(3)).each(function(prop) {
        var parts = prop.split(':');
        obj.properties[parts[0]] = parts[1];
      });
      var b64 = new Buffer(JSON.stringify(obj)).toString('base64');
      if (options.dryRun) {
        console.log(chalk.gray('Event data:'));
        console.log(JSON.stringify(obj, null, 2));
        console.log();
        console.log(chalk.gray('Endpoint:'));
        console.log('http://api.mixpanel.com/import/?api_key=' + options.apiKey + '&data=' + b64);
        console.log();
      } else {
        requestCount++;
        request.post('http://api.mixpanel.com/import/?api_key=' + options.apiKey + '&data=' + b64, function(err, response, body) {
          if (options.log) {
            if (err) {
              console.log(chalk.red('Error:', err));
            } else if (response.statusCode !== 200) {
              console.log(chalk.red('Error: mixpanel returned status', response.statusCode));
            } else if (response.statusCode === 200) {
              console.log(chalk.green('Successfully fired an event.'));
            } else {
              console.log(chalk.red('Something unexpected happened...'));
            }
          }
          requestCount--;
        });
      }
    }).on('end', function() {
      async.whilst(function() { return requestCount > 0 }, function(next) {
        setTimeout(next, 200);
      }, function(err) {
        console.log(chalk.green('Data successfully imported.'));
        process.exit(0);
      });
    });
    stream.pipe(input);
  });

program.parse(process.argv);

module.exports = program;
