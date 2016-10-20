var express = require('express');
var router = express.Router();
var MongoClient = require('mongodb').MongoClient;

var dburl = 'mongodb://localhost:27017/local';

/* GET data in the DB initially. */
router.get('/', function(req, res, next) {
	MongoClient.connect(dburl, function(err, db) {
		if(err) {
			console.log('Connecting to DB failed');
			res.send([]);
		} else {
			var col = db.collection('tweets');
			if(col) {
				// only fetch the most recent 24 hrs
				col.find({
					created_at: {$gt: new Date(Date.now() - 24*60*60*1000)}
				}).toArray(function(err, docs) {
					if(err) {
						console.log('Fetching documents failed');
						res.send([]);
					} else {
						console.log('Loading tweets happens here');
						res.send(docs);
					}
				});
			} else {
				res.send([]);
			}
		}
	});
});

module.exports = router;
