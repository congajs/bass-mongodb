/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// bass modules
const BassConnection = require('bass').Connection;

// local modules
const IdStrategy = require('./id-strategy');

/**
 * The Mongodb adapter's Connection class
 * @param {*} connection
 * @constructor
 */
module.exports = class Connection extends BassConnection {

	/**
	 *
	 * @param  {Metadata} metadataRegistry
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot(metadataRegistry, cb) {

		var i,
			metadata,
			self = this;

		for (i in metadataRegistry.metas) {

			metadata = metadataRegistry.metas[i];

			if (metadata) {
				metadata.indexes.single.forEach(function(index) {
					self.collection(metadata.collection, function(err, collection) {
						collection.ensureIndex({ fieldName : index.field, unique : index.isUnique, sparse : index.isSparse });
					});
				});
			}
		}

		cb(null);
	}

	/**
	 * Get a collection by name
	 *
	 * @param  {String}   name
	 * @param  {Function} cb
	 * @return {void}
	 */
	collection(name, cb) {
		this.connection.collection(name, cb);
	}

	/**
	 * Instantiate a new IdStrategy instance
	 * @param {String|*} idStrategy
	 * @returns {IdStrategy}
	 */
	createIdStrategy(idStrategy) {
		return new IdStrategy(idStrategy);
	}

}