var Twitter = require('twitter');
var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var dbserver = 'localhost';
var dbport = 27017;
var db;

var AWS = require('aws-sdk'),
	awsCredentialsPath = './credentials.json',
	util = require('util');
// configure AWS
AWS.config.loadFromPath(awsCredentialsPath);

var sqs = new AWS.SQS();

var stall = true
var twitterStream;

var crawler = function(io) {
	// init database
	var dbClient = new MongoClient(new Server(dbserver, dbport));
	dbClient.open(function(err, client) {
		if(err) {
			console.log(err);
		} else {
			db = client.db('local');
			if(!db.collection('tweets')) {
				db.createCollection('tweets', function(err, col) {
					if(err) {
						console.log('Creating collection failed');
					} else {
						console.log('Collection \'tweets\' created');
					}
				});
			}
		}
	});

	// init twitter API
	function init_crawler() {
		var client = new Twitter({
			consumer_key: '54UnX3MGe9BwL2bsc6SmSAvof',
			consumer_secret: '7GUcVxhhsvy9BelEgQNKEmQaqFxkrjBEIDs7MJOnqyEMmvnwCr',
			access_token_key: '88438155-4QWKmbDIfNLMGr36n02TXGRJnn8Sl7uTvnUUNyR6J',
			access_token_secret: 'Cewa9Y9jguMC1u3aSa4YlPfnaksKlew79moTjMpUHghVW'
		});
		
		client.stream('statuses/filter', {'locations': '-180, -90, 180, 90'}, function(stream) {
			twitterStream = stream;
			stream.on('data', function(tweet) {
				// still receiving data
				stall = false
				// only record tweets with location info
				if(tweet.coordinates && tweet.coordinates.coordinates) {
					var item = {
						text: tweet.text,
						coordinates: tweet.coordinates.coordinates,
						created_at: new Date(tweet.created_at),
						sentiment: null
					};
					// store in database
					db.collection('tweets').insert(item, function(err, result) {
						if(err) {
							console.log('Inserting doc failed');
						}
						else{
							var params = {
								MessageBody: tweet.text, /* required */
								QueueUrl: 'https://sqs.us-west-2.amazonaws.com/268636140029/cloud', /* required */
								DelaySeconds: 0,
								MessageAttributes: {
								    tweetID: {
								      DataType: 'String', /* required */
								      StringValue: result[0]._id.toHexString()
								    }
								  }
							};
							sqs.sendMessage(params, function(err, data) {
								if (err) console.log(err, err.stack); // an error occurred
								// else console.log(data);           // successful response
							});
						}
					});
					// push to clients
					// actually don't push here, push after sentiment is received in sentiment.js
					// io.emit('data', item);
				}
			});

			stream.on('error', function(error) {
				console.log(error);
			});
		});
	}
	
	init_crawler();

	setInterval(function() {
		var client = new Twitter({
			consumer_key: 'ZcwM34t9B2dJdmVi0y8bUK0vH',
			consumer_secret: '7d7Bd6BhsyN1D4CGCoRDsH7tMWW0ddaXdpyp59Q2VztYVT8COV',
			access_token_key: '88438155-B1nAMQb4GvwJjbEeIUZHwhbcKqHqLwGJ6dEXkVxhQ',
			access_token_secret: 'UkF4LqFMVblakaa2esiojawxXPs2bkss3d6blJysZxoFs'
		});
		var globalWOEID = '1';
		var USAWOEID = '234224977';
//		var trendNames = [];
		var trendNames = ['a', 'b', 'c', 'd', 'Pfdcvgjnsjuydhbjkdjh', 'fdcgsyascdgvfhcjvbghfdj', 'Pfdcvgjnsjuydhbjkdjh', 'fdcgsyascdgvfhcjvbghfdj', 'Pfdcvgjnsjuydhbjkdjh', 'fdcgsyascdgvfhcjvbghfdj', 'Pfdcvgjnsjuydhbjkdjh', 'fdcgsyascdgvfhcjvbghfdj'];
//		client.get('trends/place', {'id' : '1'}, function(err, trendsArray, response) {
//			if(err) {
//				console.log(err);
//			}
//			trendsArray[0].trends.forEach(function(trend) {
//				trendNames.push(trend.name);
//			});
			io.emit('trends', trendNames);
//		});
	}, 1000);

	// remove old tweets, run every 24hrs
	setInterval(function() {
		// remove tweets 24hrs before
		var params = {
			created_at: {$lt: new Date(Date.now() - 24*60*60*1000)}
		};
		db.collection('tweets').remove(params, function(err, result) {
			if(err) {
				console.log('Removing old tweets failed');
			} else {
				console.log(result + ' old tweets removed');
			}
		});
	}, 24*60*60*1000);

	// check if no data received in 90 secs
	setInterval(function() {
		if(!stall) {
			stall = true;
			return;
		}
		twitterStream.destroy();
		setTimeout(function() {
			init_crawler();
		}, 90*1000);
	}, 90*1000);
};

module.exports = crawler;
