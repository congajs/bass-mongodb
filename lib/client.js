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
const QueryResult = require('../../bass').QueryResult;

const ObjectID = require('mongodb').ObjectID;

/**
 * The Client class for MongoDB
 *
 * @param {Connection} db
 * @constructor
 */
module.exports = class Client {

	constructor(db, logger) {
		this.db = db;
		this.logger = logger;
	}

	/**
	 * Insert a new document
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {Object}     data
	 * @param  {Function}   cb
	 * @return {void}
	 */
	insert(metadata, collection, data, cb) {

		this.logger.debug('[bass-mongodb] - insert [' + collection + ']: ' + JSON.stringify(data));

		this.db.collection(collection, function(err, coll) {

			coll.insert(data, function(err, docs){

				var idFieldName = metadata.getIdFieldName();
				if (idFieldName && idFieldName.length === 0) {

					data[idFieldName] = docs[0][idFieldName];

				}

				cb(err, data);

			});
		}); 
	}

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
	update(metadata, collection, id, data, cb) {

		this.logger.debug('[bass-mongodb] - update [' + collection + ']: ' + ' - ' + id + ' : ' + JSON.stringify(data));

		var idFieldName = metadata.getIdFieldName();
		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			this.db.collection(collection, function(err, coll) {

				var cond = {};
				cond[idFieldName] = new ObjectID(id);

				if (typeof data[metadata.versionProperty] !== 'undefined' && data[metadata.versionProperty])
				{
					cond[metadata.versionProperty] = data[metadata.versionProperty] - 1;
				}

				// need to remove the id from the update data
				delete data[idFieldName];

				// cb(err, docs)
				coll.update(cond, {'$set' : data }, (err, doc) => {

					if (err) {
						return cb(err);
					}

					cb(null, doc);
				});
			});
		}
	}

	/**
	 * Update documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Object}    data
	 * @param  {Function}  cb
	 * @return {void}
	 */
	updateBy(metadata, collection, criteria, data, cb) {

		if (typeof criteria === 'undefined'){
			criteria = {};
		}

		// NOTE : we can't keep track of the version or updatedAt with this method

		this.db.collection(collection, function(err, coll){

			if (err) {

				cb(err, 0);

			} else {

				// cb(err, numberOfRemovedDocuments)
				coll.update(criteria, {'$set': data}, {multi: true}, cb);
			}
		});
	}

	/**
	 * Remove document(s) by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	remove(metadata, collection, id, cb) {

		var idFieldName = metadata.getIdFieldName();
		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			var cond = {};
			cond[idFieldName] = id;

			// cb(err, numberOfRemovedDocuments)
			this.removeOneBy(metadata, collection, cond, cb);
		}
	}

	/**
	 * Remove documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Function}  cb
	 * @return {void}
	 */
	removeOneBy(metadata, collection, criteria, cb) {

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
	}

	/**
	 * Remove documents by simple criteria
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Object}    criteria
	 * @param  {Function}  cb
	 * @return {void}
	 */
	removeBy(metadata, collection, criteria, cb) {

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
	}

	/**
	 * Find a document by id
	 * 
	 * @param  {Metadata}   metadata
	 * @param  {string}     collection
	 * @param  {ObjectID}   id
	 * @param  {Function}   cb
	 * @return {void}
	 */
	find(metadata, collection, id, cb) {

		var idFieldName = metadata.getIdFieldName();

		if (!idFieldName || idFieldName.length === 0) {

			cb(new Error('Could not find the Bass ID Field for ' + collection));

		} else {

			var start = new Date();

			this.db.collection(collection, (err, coll) => {

				var cond = {};
				cond[idFieldName] = id;

				const time = new Date() - start;
				
				this.logger.debug('[bass-mongodb] - find [' + collection + ']: ' + id + ' : ' + time + 'ms');

				// cb(err, item)
				coll.findOne(cond, cb);
			});
		}
	}

	/**
	 * Find documents based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findByQuery(metadata, collection, query, cb) {

		const self = this;
		const mongoCriteria = this.convertQueryToCriteria(query);

		const start = new Date();

		this.db.collection(collection, function(err, coll){

			// get the mongo cursor
			const cursor = coll.find(mongoCriteria);

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
			const queryResult = new QueryResult(query);

			// use a callback for finalization to support cursor.count()
			const finish = function(callback){

				// get our results as an array of documents
				cursor.toArray(function(err, documents){

					// add documents to the query result
					queryResult.setData(documents);

					const time = new Date() - start;
					
					self.logger.debug('[bass-mongodb] - findByQuery [' + collection + ']: ' + JSON.stringify(query) + ' : ' + time + 'ms');

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
	}

	/**
	 * Get a document count based on a Query
	 * 
	 * @param  {Metadata} metadata
	 * @param  {string}   collection
	 * @param  {Query}    query
	 * @param  {Function} cb
	 * @return {void}
	 */
	findCountByQuery(metadata, collection, query, cb) {

		var mongoCriteria = this.convertQueryToCriteria(query);

		this.db.collection(collection, function(err, coll){

			var cursor = coll.find(mongoCriteria).count(function(err, count){
				cb(err, count);
			});
		});
	}

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
	findBy(metadata, collection, criteria, sort, skip, limit, cb) {

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
	}

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
	findWhereIn(metadata, field, values, sort, limit, cb) {

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
	}

	/**
	 * Create a collection
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {Function}  cb
	 * @return {void}
	 */
	create(metadata, collection, cb) {

		this.db.createCollection(collection, cb);
	}

	/**
	 * Drop a collection
	 *
	 * @param  {Metadata}  metadata
	 * @param  {String}   collection
	 * @param  {Function} cb
	 * @return {void}
	 */
	drop(metadata, collection, cb) {

		this.db.collection(collection, function(err, coll){
			coll.drop(cb);
		});
	}

	/**
	 * Rename a collection
	 * 
	 * @param  {Metadata}  metadata
	 * @param  {String}    collection
	 * @param  {String}    newName
	 * @param  {Function}  cb
	 * @return {void}
	 */
	rename(metadata, collection, newName, cb) {
		this.db.collection(collection, function(err, coll){
			coll.rename(newName, cb);
		});
	}

	/**
	 * Get a list of all of the collection names in the current database
	 * 
	 * @param  {Function} cb
	 * @return {void}
	 */
	listCollections(cb) {
		this.db.collections(cb);
	}

	/**
	 * Convert a Bass Query to MongoDB criteria format
	 *
	 * @param  {Query} query
	 * @return {Object}
	 */
	convertQueryToCriteria(query) {

		var newQuery = {};

		var conditions = query.getConditions();

		var i, field;

		for (field in conditions){

			if (typeof conditions[field] === 'object' &&
				conditions[field].constructor.name !== 'ObjectID' &&
				conditions[field].constructor.name !== 'ObjectId'){

				var tmp = {};

				for (i in conditions[field]){
					// '$i' is our operator - $gt, $lt, etc.
					tmp['$' + i] = conditions[field][i];
				}

				// ex. {field: {$gt: 5}}, {field: {$nin: [1,2,3}}, etc.
				newQuery[field] = tmp;

			} else {

				// checking against an ObjectId or scalar, using equals
				newQuery[field] = conditions[field];

			}
		}

		return newQuery;
	}
}