/*
 * This file is part of the bass-nedb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Connection = function(connection){
	this.connection = connection;
};

Connection.prototype = {

	/**
	 * 
	 * @param  {Metadata} metadata
	 * @param  {Function} cb
	 * @return {void}
	 */
	boot: function(metadataRegistry, cb){

		var that = this;

		for (var i in metadataRegistry.metas){

			var metadata = metadataRegistry.metas[i];

			metadata.indexes.single.forEach(function(index){
				that.collection(metadata.collection, function(err, collection){
					collection.ensureIndex({ fieldName : index.field, unique : index.isUnique, sparse : index.isSparse });
				});
			});
		}

		cb(null);
	},

	/**
	 * Get a collection by name
	 * 
	 * @param  {String}   name
	 * @param  {Function} cb
	 * @return {void}
	 */
	collection: function(name, cb){
		this.connection.collection(name, cb);
	}

};

module.exports = Connection;