/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Db = require('mongodb').Db;
var Server = require('mongodb').Server;

var Connection = require('./connection');

var optionKeys = ['poolSize'];
var optionKeyLen = optionKeys.length;

module.exports = {

	factory: function(config, cb){

		// TODO : more options (http://mongodb.github.io/node-mongodb-native/driver-articles/mongoclient.html?highlight=pool)
		var options = {};
		for (var i = 0; i < optionKeyLen; i++) {

			var key = optionKeys[i];

			if (typeof config[key] !== 'undefined') {
				if (!isNaN(config[key])) {

					options[key] = parseInt(config[key], 10);

				} else {

					options[key] = config[key];

				}
			}
		}

		var client = new Db(config.database, new Server(config.host, config.port, options), {w: 1});

		client.open(function(err, p_client){
			cb(err, new Connection(p_client));
		});
	}
};