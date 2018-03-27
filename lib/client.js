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
const QueryResult = require('bass').QueryResult;

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

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            if (err) {
                cb(err, null);
                return;
            }

            coll.insert(data, (err, docs) => {

                if (err) {
                    cb(err, null);
                    return;
                }

                const idFieldName = metadata.getIdFieldName();
                if (idFieldName && idFieldName.length === 0) {

                    data[idFieldName] = docs[0][idFieldName];

                }

                this.logger.debug(
                    '[bass-mongodb] - insert [' + collection + ']: ' + JSON.stringify(data)
                    + ' : ' + ((new Date()) - start) + 'ms');

                cb(null, data);

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

        const start = new Date();

        const idFieldName = metadata.getIdFieldName();
        if (!idFieldName || idFieldName.length === 0) {

            cb(new Error('Could not find the Bass ID Field for ' + collection));

        } else {

            this.db.collection(collection, (err, coll) => {

                if (err) {
                    cb(err, null);
                    return;
                }

                if (!(data instanceof Object)) {
                    data = {};
                } else {
                    data = Object.create(data);
                }

                const cond = {};
                cond[idFieldName] = new ObjectID(id);

                if (data[metadata.versionProperty]) {
                    cond[metadata.versionProperty] = data[metadata.versionProperty] - 1;
                }

                // need to remove the id from the update data
                delete data[idFieldName];

                // cb(err, docs)
                coll.updateOne(cond, {'$set' : data }, (err, data) => {

                    this.logger.debug(
                        '[bass-mongodb] - update [' + collection + ']: ' + ' - ' + id + ' : ' +
                        JSON.stringify(data) + ' : ' + ((new Date()) - start) + 'ms');

                    cb(err, data && data.result);

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

        const start = new Date();

        if (!(criteria instanceof Object)) {
            criteria = {};
        }

        // NOTE : we can't keep track of the version or updatedAt with this method

        this.db.collection(collection, (err, coll) => {

            if (err) {
                cb(err, 0);
                return;
            }

            data = Object.create(data);

            let $set = {};
            if ('$set' in data) {
                $set = data['$set'] || {};
                delete data['$set'];
            }

            const update = {};
            for (const m in data) {
                if (m[0] !== '$') {
                    $set[m] = data[m];
                } else {
                    update[m] = data[m];
                }
            }

            if (Object.keys($set).length !== 0) {
                update['$set'] = $set;
            }

            // cb(err, numberOfRemovedDocuments)
            coll.updateMany(criteria, update, (err, result) => {

                this.logger.debug(
                    '[bass-mongodb] - updateBy [' + collection + ']: ' + ' - '
                    + JSON.stringify(criteria) + ' : ' + JSON.stringify(update)
                    + ' : ' + JSON.stringify(result) + ' : ' + ((new Date()) - start) + 'ms');

                cb(err, result && result.modifiedCount || 0);
            });
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

        const idFieldName = metadata.getIdFieldName();
        if (!idFieldName || idFieldName.length === 0) {

            cb(new Error('Could not find the Bass ID Field for ' + collection));

        } else {

            if (typeof id === 'string') {
                id = new ObjectID(id);
            }

            const cond = {};
            cond[idFieldName] = id;

            // cb(err, numberOfRemovedDocuments)
            this.removeOneBy(metadata, collection, cond, (err, data) => {
                cb(err, data && data.deletedCount || 0);
            });
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

        const start = new Date();

        if (!(criteria instanceof Object)) {
            criteria = {};
        }

        this.db.collection(collection, (err, coll) => {

            if (err) {

                cb(err, 0);

            } else {

                // cb(err, numberOfRemovedDocuments)
                coll.deleteOne(criteria, (err, data) => {

                    this.logger.debug(
                        '[bass-mongodb] - removeOneBy [' + collection + ']: ' + ' - '
                        + JSON.stringify(criteria) + ' : ' + ((new Date()) - start) + 'ms');

                    cb(err, data && data.deletedCount || 0);
                });
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

        const start = new Date();

        if (!criteria) {
            criteria = {};
        }

        this.db.collection(collection, (err, coll) => {

            if (err) {

                cb(err, 0);

            } else {

                // cb(err, numberOfRemovedDocuments)
                coll.deleteMany(criteria, (err, data) => {

                    this.logger.debug(
                        '[bass-mongodb] - removeBy [' + collection + ']: ' + ' - '
                        + JSON.stringify(criteria) + ' : ' + ((new Date()) - start) + 'ms');

                    cb(err, data && data.deletedCount || 0);
                });
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

            const start = new Date();

            this.db.collection(collection, (err, coll) => {

                const cond = {};
                cond[idFieldName] = id;

                const time = new Date() - start;

                this.logger.debug(
                    '[bass-mongodb] - find [' + collection + ']: ' + id + ' : ' + time + 'ms');

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

        const mongoCriteria = this.convertQueryToCriteria(query);

        const start = new Date();

        this.db.collection(collection, (err, coll) => {

            // get the mongo cursor
            const cursor = coll.find(mongoCriteria);

            // apply "pagination" to cursor
            if (query.getSort() !== null){
                cursor.sort(query._sort);
            }

            // initialize a query result for our response
            const queryResult = new QueryResult(query);

            // use a callback for finalization to support cursor.count()
            const finish = (callback) => {

                if (query.getSkip() !== null){
                    cursor.skip(query.getSkip());
                }

                if (query.getLimit() !== null){
                    cursor.limit(query.getLimit());
                }

                // get our results as an array of documents
                cursor.toArray((err, documents) => {

                    // add documents to the query result
                    queryResult.setData(documents);

                    this.logger.debug(
                        '[bass-mongodb] - findByQuery [' + collection + ']: ' +
                        	JSON.stringify(query) + ' : ' + ((new Date()) - start) + 'ms');

                    // execute callback with query result
                    callback(err, queryResult);

                });
            };

            // if we are told to, fetch the total count
            if (query.getCountFoundRows()){

                cursor.count((err, count) => {
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

    	const start = new Date();

        const mongoCriteria = this.convertQueryToCriteria(query);

        this.db.collection(collection, (err, coll) => {

            coll.find(mongoCriteria).count((err, count) => {

                this.logger.debug(
                    '[bass-mongodb] - findCountByQuery [' + collection + ']: ' +
                    	JSON.stringify(query) + ' : ' + ((new Date()) - start) + 'ms');

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

    	const start = new Date();

        if (!criteria) {
            criteria = {};
        }

        this.db.collection(collection, (err, coll) => {

            if (err) {
                cb(err, null);
                return;
            }

            const cursor = coll.find(criteria);

            if (sort) {
                cursor.sort(sort);
            }

            if (skip && !isNaN(skip)) {
                cursor.skip(skip);
            }

            if (limit && !isNaN(limit)) {
                cursor.limit(limit);
            }

            cursor.toArray((err, items) => {

                this.logger.debug(
                    '[bass-mongodb] - findBy [' + collection + ']: ' +
                    	JSON.stringify(criteria) + ' : ' + ((new Date()) - start) + 'ms');

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

        this.db.collection(collection, (err, coll) => {
        	if (err) {
        		cb(err);
        		return;
			}
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

        const newQuery = {};

        const conditions = query.getConditions();

        for (let field in conditions){

            if (typeof conditions[field] === 'object' &&
                conditions[field].constructor.name !== 'ObjectID' &&
                conditions[field].constructor.name !== 'ObjectId'){

                const tmp = {};

                for (let i in conditions[field]){
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
};
