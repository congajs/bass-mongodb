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
var MongoClient = require('mongodb').MongoClient;


var Connection = require('./connection');

var optionKeys = ['poolSize','auto-reconnect'];
var optionKeyLen = optionKeys.length;

/**
 * Each client of this module will end up with its own server collection.
 * @type {Array}
 */
var mongoClients = {};

/**
 * asyncLock ensures that a connection is open when multiple requests
 * are received for the same host.
 *
 * TODO: We need to find a home for this. This is useful for any
 * occasion where we want to ensure a resource is initialized and available.
 */
function asyncLock() {}

asyncLock.prototype = {

	locked: [],

	waitAsync: function(hash, cb, args) {

		var self = this;
		var found = false;

		if (this.locked.length > 0) {

			for (var i = 0; i < this.locked.length; i++) {

				found = this.locked[i] == hash;
				if (found) { break; }
			}
		}

		if (found) {

			// Use setImmediate instead of nextTick to ensure that IO is drained.
			setImmediate(function(args){

				self.waitAsync(hash, cb, args);
			}, args)
		} else {

			cb.apply(this, args);
		}
	},

	beginAsync: function(hash) {

		var found = false;
		for (var i = 0; i < this.locked.length; i++) {

			found = this.locked[i] == hash;
			if (found) { break; }
		}

		if (!found) { this.locked.push(hash) }
	},

	endAsync : function(hash) {

		for (var i = 0; i < this.locked.length; i++) {

			if (this.locked[i] == hash) {

				this.locked.splice(i, 1);
				break;
			}
		}
	}
};

// Global async lock instance
var _lock = new asyncLock();

module.exports = {

	factory: function(config, cb){

		var options = {};
		var optionsHash = '';

		for (var i = 0; i < optionKeyLen; i++) {

			var key = optionKeys[i];
			if (typeof config[key] !== 'undefined') {

				if (!isNaN(config[key])) {

					options[key] = parseInt(config[key], 10);

				} else {

					options[key] = config[key];
				}

				// We calculate a hash of the options
				// If they are different then a new connection pool is created.
				optionsHash += ':' + key + ':' + options[key];
			}
		}

		// Normalize host if a loopback address is given
		if (config.host == 'localhost') {

			config.host = '127.0.0.1';
		}

		// See if the pool has been given a name
		var poolName = '';
		if (typeof config['poolName'] !== 'undefined') {

			poolName = ':' + config['poolName'] + ':';
		}

		var connectionHash = config.host + ':' + config.port + poolName + optionsHash;

		var connectToDb = function(client) {

			var db = new Connection(client.db(config.database));
			if (!db) {

				cb(new Error('Unable to connect to database'));
			} else {

				cb(null, db);
			}
		};

		if (!mongoClients[connectionHash]) {

			mongoClients[connectionHash] = new MongoClient(new Server(config.host, config.port, options));

			_lock.beginAsync(connectionHash);
            mongoClients[connectionHash].open(function(err, client){

				_lock.endAsync(connectionHash);
				if (err) {

					console.log(err)
					cb(err);
				} else {

					connectToDb(client);
				}
			});

		} else {

			_lock.waitAsync(connectionHash, connectToDb, [ mongoClients[connectionHash] ]);
		}
	}
};