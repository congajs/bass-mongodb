/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

/**
 * The Query Result Class
 * @type {QueryResult} Class Constructor
 */
var QueryResult = require('bass').QueryResult;

/**
 * The Client class for MongoDB
 *
 * @param {Connection} db
 * @constructor
 */
function Client(db){
	this.db = db;
}

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

		//console.log('collection: ' + collection);
		//console.log(this.db);

		this.db.collection(collection, function(err, coll) {
			coll.insert(data, function(err, docs){

				var idFieldName = metadata.getIdFieldName();
				if (idFieldName && idFieldName.length === 0) {

					data[idFieldName] = docs[0][idFieldName];

				}

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
		var idFieldName = metadata.getIdFieldName();
		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			this.db.collection(collection, function(err, coll) {

				var cond = {};
				cond[idFieldName] = id;

				if (typeof data[metadata.versionProperty] !== 'undefined' && data[metadata.versionProperty])
				{
					cond[metadata.versionProperty] = data[metadata.versionProperty] - 1;
				}

				// need to remove the id from the update data
				delete data[idFieldName];

				// cb(err, docs)
				coll.update(cond, {'$set' : data }, cb);
			});
		}
	},

	/**
	 * Remove document(s) by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove: function(metadata, collection, id, cb) {

		var idFieldName = metadata.getIdFieldName();
		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			var cond = {};
			cond[idFieldName] = id;

			// cb(err, numberOfRemovedDocuments)
			this.removeOneBy(metadata, collection, cond, cb);
		}
	},

	/**
	 * Remove documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Function}  cb
	 * @return {void}
	 */
	removeOneBy: function(metadata, collection, criteria, cb){

		if (typeof criteria === 'undefined'){
			criteria = {};
		}

		this.db.collection(collection, function(err, coll){

			if (err) {

				cb(err, 0);

			} else {

				// cb(err, numberOfRemovedDocuments)
				coll.remove(criteria, {single: 1}, cb);
			}
		});
	},

	/**
	 * Remove documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Function}  cb
	 * @return {void}
	 */
	removeBy: function(metadata, collection, criteria, cb){

		if (typeof criteria === 'undefined'){
			criteria = {};
		}

		this.db.collection(collection, function(err, coll){

			if (err) {

				cb(err, 0);

			} else {

				// cb(err, numberOfRemovedDocuments)
				coll.remove(criteria, {}, cb);
			}
		});
	},

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find: function(metadata, collection, id, cb){
		var idFieldName = metadata.getIdFieldName();
		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			this.db.collection(collection, function(err, coll){

				var cond = {};
				cond[idFieldName] = id;

				// cb(err, item)
				coll.findOne(cond, cb);
			});
		}
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

			// get the mongo cursor
			var cursor = coll.find(mongoCriteria);

			// apply "pagination" to cursor
			if (query.getSort() !== null){
				cursor.sort(query._sort);
			}
			
			if (query.getSkip() !== null){
				cursor.skip(query.getSkip());
			}

			if (query.getLimit() !== null){
				cursor.limit(query.getLimit());
			}

			// initialize a query result for our response
			var queryResult = new QueryResult(query);

			// use a callback for finalization to support cursor.count()
			var finish = function(callback){

				// get our results as an array of documents
				cursor.toArray(function(err, documents){

					// add documents to the query result
					queryResult.setData(documents);

					// execute callback with query result
					callback(err, queryResult);

				});
			};

			// if we are told to, fetch the total count
			if (query.getCountFoundRows()){

				cursor.count(function(err, count){
					if (err){

						cb(err, null);
					} else {

						queryResult.totalRows = count;

						finish(cb);
					}
				});

			} else {

				finish(cb);

			}
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

			var cursor = coll.find(mongoCriteria).count(function(err, count){
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

			if (err) {
				cb(err, null);
				return;
			}

			var cursor = coll.find(criteria);

			if (typeof sort !== 'undefined'){
				cursor.sort(sort);
			}
			
			if (skip && !isNaN(skip)) {
				cursor.skip(skip);
			}

			if (limit && !isNaN(limit)) {
				cursor.limit(limit);
			}

			cursor.toArray(function(err, items){
				cb(err, items);
			});
		});
	},

	/**
	 * Find documents where a field has a value in an array of values
	 *
	 * @param {Metadata} metadata The metadata for the document type you are fetching
	 * @param {String} field The document's field to search by
	 * @param {Array.<(String|Number)>} values Array of values to search for
	 * @param {Object|null} sort Object hash of field names to sort by, -1 value means DESC, otherwise ASC
	 * @param {Number|null} limit The limit to restrict results
	 * @param {Function} cb Callback function
	 */
	findWhereIn: function(metadata, field, values, sort, limit, cb) {

		var criteria = {};
		criteria[field] = {'$in' : values};

		this.findBy(
			metadata,
			metadata.collection,
			criteria ,
			sort,
			null,
			limit || undefined,
			cb
		);
	} ,

	/**
	 * Create a collection
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Function}  cb
	 * @return {void}
	 */
	create: function(metadata, collection, cb){

		this.db.createCollection(collection, cb);
	},

	/**
	 * Drop a collection
	 *
	 * @param  {Metadata}  metadata
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

		var i, field;

		for (field in conditions){

			if (typeof conditions[field] === 'object' &&
				conditions[field].constructor.name !== 'ObjectID' &&
				conditions[field].constructor.name !== 'ObjectId'){

				var tmp = {};

				for (i in conditions[field]){
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


Client.prototype.constructor = Client;

module.exports = Client;