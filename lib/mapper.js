/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
const _ = require('lodash');
const async = require('async');

const { DBRef, ObjectID, Binary } = require('mongodb');

const { AdapterMapper } = require('bass');

module.exports = class Mapper extends AdapterMapper {

	/**
	 * Map an object of criteria to the correct types for
	 * the database to use
	 *
	 * @param  {Metadata} metadata
	 * @param  {Object} criteria
	 * @return {Object}
	 */
	mapCriteriaToDatabase(metadata, criteria) {

		var dbCriteria = {};

		if (criteria) {
			for(var i in metadata.fields) {
				if (typeof criteria[metadata.fields[i].property] !== 'undefined') {

					dbCriteria[metadata.fields[i].name] = this.convertModelValueToDbValue(
						metadata.fields[i].type,
						criteria[metadata.fields[i].property]);

				} else if (metadata.fields[i].type === 'object') {

					for (var m in criteria) {
						if (m.indexOf('.') !== -1 &&
							m.substr(0, metadata.fields[i].name.length) === metadata.fields[i].name) {

							dbCriteria[m] = this.convertModelValueToDbValue(
								metadata.fields[i].type,
								criteria[m]);

						}
					}

				}
			}
		}

		return dbCriteria;
	}

	/**
	 * Convert a dot notation (such as used in find) to the appropiate mapped field name
	 * @param {Metadata} metadata
	 * @param {String} property
	 */
	mapMetadataField(metadata, property) {
        const parts = property.split('.');
        if (parts.length !== 0) {
            const field = metadata.getFieldByProperty(parts[0]);
            if (field && field.type === 'object') {
                return field;
            }
        }
        return metadata.getFieldByProperty(property);
	}

	/**
	 * Convert a Javascript value to a db value
	 *
	 * @param {String} type the field type (string, number, objectid, etc...)
	 * @param  {*} value
	 * @return {*}
	 */
	convertModelValueToDbValue(type, value) {

		var converted = value;

		switch (type){
			case 'objectid':
				if (!(value instanceof Object) && value !== null) {
					converted = new ObjectID(value);
				}
				break;

			case 'date':
				if (!(value instanceof Date) && value !== null) {
					converted = new Date(value);
					if (converted.toString() === 'Invalid Date') {
						converted = value;
					}
				}
				break;

			case 'binary':
				if (!(value instanceof Binary) && value !== null) {
					if (!Buffer.isBuffer(value)) {
						try {
							value = Buffer.from(value);
						} catch (e) {
							console.error(e.stack || e);
						}
					}
					value = new Binary(value);
				}
				break;
		}

		return converted;
	}

	/**
	 * Convert a db value to a Javascript value
	 *
	 * @param {String} type the field type (string, number, objectid, etc...)
	 * @param  {*} value
	 * @return {*}
	 */
	convertDbValueToModelValue(type, value) {

		if (value === undefined || value === null) {
			return value;
		}

		var converted = value;

		switch (type) {
			case 'objectid' :
				if (value instanceof ObjectID || value.constructor === ObjectID || value.constructor.name === 'ObjectID') {
					converted = value.toHexString();
				}
				break;

			case 'date' :
				if (typeof value === 'string') {
					converted = new Date(value);
					if (converted.toString() === 'Invalid Date') {
						converted = value;
					}
				}
				break;

			case 'binary' :
				if (value instanceof Binary) {
					converted = Buffer.from(value.buffer);
				}
				break;
		}

		return converted;
	}

	/**
	 * Convert relations on a model to data to insert
	 * 
	 * @param  {MetaData} metadata
	 * @param  {Object}   model
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	convertModelRelationsToData(metadata, model, data, cb) {

		var i,
			relation,
			relationMetadata;

		// one-to-one
		for (i in metadata.relations['one-to-one']){

			relation = metadata.relations['one-to-one'][i];
			relationMetadata = this.registry.getMetadataByName(relation.document);
			
			if (typeof model[i] !== 'undefined' && model[i] !== null) {
				data[relation.column] = new DBRef(relationMetadata.collection, model[i].id);
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){
			relation = metadata.relations['one-to-many'][i];
			relationMetadata = this.registry.getMetadataByName(relation.document);

			data[relation.field] = [];

			model[i].forEach(function(oneToManyDoc){
				data[relation.field].push(new DBRef(relationMetadata.collection, oneToManyDoc.id));
			});
		}

		if (typeof cb === 'function') {
			cb(null, data);
		}
	}

	/**
	 * Map raw data to a model using sparse information for any joins
	 * so that they can be grabbed later on in bulk and merged in
	 *
	 * @param  {Object}   model
	 * @param  {Metadata} metadata
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	mapPartialRelationsToModel(model, metadata, data, cb) {

		var relations = metadata.getRelations();

		var keys = Object.keys(metadata.relations['one-to-one']);
		for (var i = 0, j = keys.length; i < j; i++) {
			var relation = relations['one-to-one'][keys[i]];
			if (data[relation.column] !== null && typeof data[relation.column] !== 'undefined') {
				model[relation.field] = data[relation.column].oid; 
			}
		}

		var keys = Object.keys(metadata.relations['one-to-many']);
		for (var i = 0, j = keys.length; i < j; i++) {
			var relation = relations['one-to-many'][keys[i]];


			model[relation.field] = data[relation.field].map(function(el){ return el.oid; });
		}

		cb(null, model);
	}

	/**
	 * Run queries on a collection of partial models and merge the related
	 * models in to each model
	 * 
	 * @param  {Manager}  manager
	 * @param  {Metadata} metadata
	 * @param  {Object}   data
	 * @param  {Function} cb
	 * @return {void}
	 */
	mergeInRelations(manager, metadata, data, cb) {

		if (metadata.relations['one-to-one'].length === 0 && metadata.relations['one-to-many'] === 0) {
			cb(null, data);
			return;
		}

		var calls = [];
		var self = this;

		this.addOneToOneCalls(manager, metadata, data, calls);
		this.addOneToManyCalls(manager, metadata, data, calls);

		async.parallel(calls, function(err) {

			if (err) {
				cb(err);
			} else {
				cb(null, data);
			}

		});
	}

	addOneToOneCalls(manager, metadata, data, calls) {

		// var start = new Date();

		var self = this;

		var keys = Object.keys(metadata.relations['one-to-one']);
		for (var i = 0, j = keys.length; i < j; i++) {

			var relation = metadata.relations['one-to-one'][keys[i]];
			var relationMetadata = self.registry.getMetadataByName(relation.document);
			var idFieldName = relationMetadata.getIdFieldName();










			(function(data, relation, relationMetadata) {

				calls.push(function(cb){

					var ids = [];

					data.forEach(function(obj) {

						var oid = null;

						// trying this for now
						// @todo - need to figure out a real way to deal with this
						// basically if relation is contained within mongo, we should use ObjectID,
						// otherwise we need to use the regular id
						try {
							oid = new ObjectID(obj[relation.field]);
						} catch (e) {
							oid = obj[relation.field];
						}

						ids.push(oid);
					});

					ids = _.uniq(ids);

					if (ids.length > 0) {

						var relationManager = manager.session.getManagerForModelName(relation.document);

						relationManager.getRepository(relation.document).getReaderClient().findWhereIn(relationMetadata, idFieldName, ids, null, null, function(err, relatedData) {

							if (err) {

								cb(err);

							} else {

								//var s = new Date();

								relationManager.mapDataToModels(relationMetadata, relatedData, function(err, documents) {

									// var e = new Date();
									// var t = e - s;

									// console.log('map data to models inside merge relations: ' + relationMetadata.name + ' - ' + t);

									if (err) {

										cb(err);

									} else {

										var docMap = {};
										var idPropertyName = relationMetadata.getIdPropertyName();
										var relationField = relation.field;

										documents.forEach(function(doc) {
											docMap[doc[idPropertyName]] = doc;
										});

										data.forEach(function(obj) {
											obj[relationField] = docMap[obj[relationField]];
										});

										cb(null);
									}
								});
							}
						});

					} else {
						cb(null);
					}
				});

			})(data, relation, relationMetadata);
		}

		// var end = new Date();
		// var time = end - start;

		// console.log('add one-to-one calls: ' + metadata.name + ' - ' + time);
	}

	addOneToManyCalls(manager, metadata, data, calls) {

		// var start = new Date();

		var self = this;

		var keys = Object.keys(metadata.relations['one-to-many']);
		for (var i = 0, j = keys.length; i < j; i++) {

			var relation = metadata.relations['one-to-many'][keys[i]];

			(function(data, relation) {

				calls.push(function(cb){

					var relationMetadata = self.registry.getMetadataByName(relation.document);
					var idFieldName = relationMetadata.getIdFieldName();

					var ids = [];

					data.forEach(function(obj) {
						obj[relation.field].forEach(function(rel) {
							ids.push(rel);
						});
					});


					ids = _.uniq(ids);

					if (ids.length > 0) {

						var relationManager = manager.session.getManagerForModelName(relation.document);


						relationManager.getRepository(relation.document).getReaderClient().findWhereIn(relationMetadata, idFieldName, ids, null, null, function(err, relatedData) {

						
							if (err) {

								cb(err);

							} else {

								relationManager.mapDataToModels(relationMetadata, relatedData, function(err, documents) {

									if (err) {

										cb(err);

									} else {

										var docMap = {};

										documents.forEach(function(doc) {
											docMap[doc[relationMetadata.getIdPropertyName()]] = doc;
										});

										data.forEach(function(obj) {
											var tmp = [];
											obj[relation.field].forEach(function(id) {
												tmp.push(docMap[id]);
											});
											obj[relation.field] = tmp;
										});

										docMap = null;

										cb(null);
									}
								});
							}
						});
					} else {
						cb(null);
					}
				});
			})(data, relation);
		}

		// var end = new Date();
		// var time = end - start;

		// console.log('add one-to-many calls: ' + metadata.name + ' - ' + time);
	}



	// convertDataRelationToDocument(metadata, fieldName, data, model, mapper, cb) {

	// 	if (typeof data[fieldName] === 'undefined' || data[fieldName] === null ) {
	// 		cb(null, model);
	// 		return;
	// 	}

	// 	if (typeof metadata.relations['one-to-one'][fieldName] !== 'undefined') {

	// 		var dbRef = new DBRef(data[fieldName].namespace, new ObjectID(data[fieldName].oid));

	// 		this.client.db.connection.dereference(dbRef, function (err, item) {
	// 			if (!err) {
	// 				model[fieldName] = item;
	// 			}
	// 			cb(err, model);
	// 		});

	// 	} else if (typeof metadata.relations['one-to-many'][fieldName] !== 'undefined') {

	// 		var ids = [];
	// 		for(var j in data[fieldName]){
	// 			ids.push(new ObjectID(data[fieldName][j].oid));
	// 		}

	// 		// do not continue if we have nothing to map
	// 		if (ids.length === 0) {
	// 			cb(null, model);
	// 			return;
	// 		}

	// 		var relation = metadata.getRelationByFieldName(fieldName);
	// 		var relationMetadata = this.registry.getMetadataByName(relation.document);
	// 		var annotation = metadata.relations['one-to-many'][fieldName];

	// 		var sort = null;
	// 		if (annotation.sort && annotation.direction) {
	// 			sort = {};
	// 			sort[annotation.sort] = annotation.direction.toString().toLowerCase() === 'desc' ? -1 : 1;
	// 		}

	// 		this.client.findWhereIn(relationMetadata, '_id', ids, sort, null, function(err, docs) {
	// 			if (!err) {
	// 				model[fieldName] = docs;
	// 			}
	// 			cb(err, model);
	// 		});

	// 	} else {

	// 		cb(null, model);
	// 	}
	// }

	// convertDataRelationsToDocument(metadata, data, model, mapper, cb) {

	// 	var i;
	// 	var self = this;
	// 	var calls = [];

	// 	// one-to-one
	// 	for (i in metadata.relations['one-to-one']){

	// 		if (typeof data[i] !== 'undefined' && data[i] !== null){

	// 			(function(d, m, idx){
	// 				calls.push(
	// 					function(callback){
	// 						self.convertDataRelationToDocument(metadata, idx, d, m, mapper, function(err, item) {
	// 							callback(err, item);
	// 						});
	// 					}
	// 				);
	// 			}(data, model, i));
	// 		}
	// 	}

	// 	// one-to-many
	// 	for (i in metadata.relations['one-to-many']){

	// 		if (typeof data[i] !== 'undefined' && data[i] !== null){

	// 			(function(d, m, idx){
	// 				calls.push(
	// 					function(callback){
	// 						self.convertDataRelationToDocument(metadata, idx, d, m, mapper, function(err, item) {
	// 							callback(err, item);
	// 						});
	// 					}
	// 				);
	// 			}(data, model, i));
	// 		}
	// 	}

	// 	if (calls.length > 0){

	// 		// run all queries and process data
	// 		async.parallel(calls, function(err, document){
	// 			cb(err, document);
	// 		});

	// 	} else {
	// 		cb(null, model);
	// 	}
	// }
}
