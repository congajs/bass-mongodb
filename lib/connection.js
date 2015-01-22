/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// bass modules
var BassConnection = require('bass').Connection;

// local modules
var IdStrategy = require('./id-strategy');

/**
 * The Mongodb adapter's Connection class
 * @param {*} connection
 * @constructor
 */
function Connection(connection){
	BassConnection.apply(this, arguments);
}
Connection.prototype = new BassConnection();
Connection.prototype.constructor = Connection;

/**
 *
 * @param  {Metadata} metadataRegistry
 * @param  {Function} cb
 * @return {void}
 */
Connection.prototype.boot = function(metadataRegistry, cb){

	var i,
		metadata,
		self = this;

	for (i in metadataRegistry.metas){

		metadata = metadataRegistry.metas[i];

		if (metadata) {
			metadata.indexes.single.forEach(function(index){
				self.collection(metadata.collection, function(err, collection){
					collection.ensureIndex({ fieldName : index.field, unique : index.isUnique, sparse : index.isSparse });
				});
			});
		}

	}

	cb(null);
};

/**
 * Get a collection by name
 *
 * @param  {String}   name
 * @param  {Function} cb
 * @return {void}
 */
Connection.prototype.collection = function(name, cb){
	this.connection.collection(name, cb);
};

/**
 * Instantiate a new IdStrategy instance
 * @param {String|*} idStrategy
 * @returns {IdStrategy}
 */
Connection.prototype.createIdStrategy = function(idStrategy) {
	return new IdStrategy(idStrategy);
};

module.exports = Connection;