const MongoClient = require('mongodb').MongoClient;


const Connection = require('./connection');

module.exports = class ConnectionFactory {


	static factory(config, logger, cb) {

		var url = 'mongodb://' + config.host + ':' + config.port + '/' + config.database;

		MongoClient.connect(url, function(err, db) {
			
			if (err !== null) {
				cb(err);
			} else {

				var connection = new Connection(db);

				cb(null, connection);

			}
		});
	}
}