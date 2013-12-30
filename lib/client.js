/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

var Client = function(db){
	this.db = db;
};

Client.prototype = {

	/**
	 * Insert a new document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert: function(metadata, collection, data, cb){

		console.log('collection: ' + collection);
		console.log(this.db);

		this.db.collection(collection, function(err, coll) {
			coll.insert(data, function(err, docs){
				data[metadata.getIdFieldName()] = docs[0][metadata.getIdFieldName()];
				cb(err, data);
			});
		}); 
	},

	/**
	 * Update a document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	update: function(metadata, collection, id, data, cb){

		this.db.collection(collection, function(err, coll) {

			var cond = {};
			cond[metadata.getIdFieldName()] = id;
			cond['version'] = data['version']-1;

			// need to remove the id from the update data
			delete data[metadata.getIdFieldName()];

			coll.update(cond, {'$set' : data }, function(err, docs){
				cb(err, docs);
			});
		}); 
	},

	/**
	 * Remove a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove: function(metadata, collection, id, cb){

		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.remove(cond, 1, function(err){
				cb(err);
			});
		});
	},

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find: function(metadata, collection, id, cb){

		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.findOne(cond, function(err, item){
				cb(err, item);
			});
		});
	},

	/**
	 * Find documents based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery: function(metadata, collection, query, cb){

		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll){

			var cursor = coll.find(mongoCriteria);

			if (query._sort !== null){
				cursor.sort(query._sort);
			}
			
			if (query.getSkip() !== null){
				cursor.skip(query.getSkip());
			}

			if (query.getLimit() !== null){
				cursor.limit(query.getLimit());
			}

			cursor.toArray(function(err, items){
				cb(err, items);
			});
		});
	},

	/**
	 * Get a document count based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery: function(metadata, collection, query, cb){

		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll){

			cursor = coll.find(mongoCriteria).count(function(err, count){
				cb(err, count);
			});
		});
	},

	/**
	 * Find documents by simple criteria
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Object}    sort
	 * @param  {Number}    skip
	 * @param  {Number}    limit
	 * @param  {Function}  cb
	 * @return {void}
	 */
	findBy: function(metadata, collection, criteria, sort, skip, limit, cb){

		if (typeof criteria === 'undefined'){
			criteria = {};
		}

		this.db.collection(collection, function(err, coll){

			var cursor = coll.find(criteria);

			if (typeof sort !== 'undefined'){
				cursor.sort(sort);
			}
			
			if (typeof skip !== 'undefined'){
				cursor.skip(skip);
			}
			
			if (typeof limit !== 'undefined'){
				cursor.limit(limit);
			}

			cursor.toArray(function(err, items){
				cb(err, items);
			});
		});
	},

	/**
	 * Create a collection
	 * 
	 * @param  {[type]}   metadata   [description]
	 * @param  {[type]}   collection [description]
	 * @param  {Function} cb         [description]
	 * @return {[type]}              [description]
	 */
	create: function(metadata, collection, cb){

		this.db.createCollection(collection, cb);
	},

	/**
	 * Drop a collection
	 * 
	 * @param  {String}   collection
	 * @param  {Function} cb
	 * @return {void}
	 */
	drop: function(metadata, collection, cb){

		this.db.collection(collection, function(err, coll){
			coll.drop(cb);
		});
	},

	/**
	 * Rename a collection
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename: function(metadata, collection, newName, cb){
		this.db.collection(collection, function(err, coll){
			coll.rename(newName, cb);
		});
	},

	/**
	 * Get a list of all of the collection names in the current database
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections: function(cb){
		this.db.collections(cb);
	},

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 * 
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria: function(query){

		var newQuery = {};

		var conditions = query.getConditions();

		for (var field in conditions){

			if (typeof conditions[field] === 'object'){

				var tmp = {};

				for (var i in conditions[field]){
					tmp['$' + i] = conditions[field][i];
				}

				newQuery[field] = tmp;

			} else {
				newQuery[field] = conditions[field];
			}
		}

		return newQuery;
	}
};

module.exports = Client;