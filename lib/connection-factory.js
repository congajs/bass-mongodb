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

module.exports = {

	factory: function(config, cb){

		var client = new Db(config.database, new Server(config.host, config.port, {}), {w: 1})

		client.open(function(err, p_client){

			cb(null, new Connection(p_client));
		});
	}
};