/*
 * This file is part of the bass-mongodb library.
 *
 * (c) Marc Roulias <marc@lampjunkie.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

// third-party modules
var async = require('async');
var DBRef = require('mongodb').DBRef;
var ObjectID = require('mongodb').ObjectID;

function Mapper(registry, client) {
	this.registry = registry;
	this.client = client;
}

Mapper.prototype = {

	/**
	 * Convert a Javascript value to a db value
	 *
	 * @param {String} type the field type (string, number, objectid, etc...)
	 * @param  {*} value
	 * @return {*}
	 */
	convertJavascriptToDb: function(type, value){

		var converted = value;

		switch (type){
			case 'objectid':
				converted = new ObjectID(value);
				break;
		}

		return converted;
	},

	/**
	 * Convert a db value to a Javascript value
	 *
	 * @param {String} type the field type (string, number, objectid, etc...)
	 * @param  {*} value
	 * @return {*}
	 */
	convertDbToJavascript: function(type, value){

		if (value === undefined || value === null) {
			return value;
		}

		var converted = value;

		if (value instanceof ObjectID || value.constructor === ObjectID || value.constructor.name === 'ObjectID') {
			converted = value.toHexString();
		}

		return converted;
	},

	convertRelationsToData: function(metadata, model, data, cb) {

		var i,
			relation,
			relationMetadata;

		// one-to-one
		for (i in metadata.relations['one-to-one']){
			relation = metadata.relations['one-to-one'][i];
			relationMetadata = this.registry.getMetadataByName(relation.document);
			data[relation.field] = new DBRef(relationMetadata.collection, model[i].id);
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
	},

	convertDataRelationToDocument: function(metadata, fieldName, data, model, mapper, cb) {

		if (typeof data[fieldName] === 'undefined' || data[fieldName] === null ) {
			cb(null, model);
			return;
		}

		if (typeof metadata.relations['one-to-one'][fieldName] !== 'undefined') {

			var dbRef = new DBRef(data[fieldName].namespace, new ObjectID(data[fieldName].oid));

			this.client.db.connection.dereference(dbRef, function(err, item){
				if (!err) {
					model[fieldName] = item;
				}
				cb(err, model);
			});

		} else if (typeof metadata.relations['one-to-many'][fieldName] !== 'undefined') {

			var ids = [];
			var relation = metadata.getRelationByFieldName(fieldName);
			var relationMetadata = this.registry.getMetadataByName(relation.document);
			var annotation = metadata.relations['one-to-many'][fieldName];

			for(var j in data[fieldName]){
				ids.push(new ObjectID(data[fieldName][j].oid));
			}

			var sort = null;
			if (annotation.sort && annotation.direction) {
				sort = {};
				sort[annotation.sort] = annotation.direction.toString().toLowerCase() === 'desc' ? -1 : 1;
			}

			this.client.findWhereIn(relationMetadata, '_id', ids, sort, null, function(err, docs) {
				if (!err) {
					model[fieldName] = docs;
				}
				cb(err, model);
			});

		} else {

			cb(null, model);
		}
	} ,

	convertDataRelationsToDocument: function(metadata, data, model, mapper, cb){

		var i;
		var self = this;
		var calls = [];

		// one-to-one
		for (i in metadata.relations['one-to-one']){

			if (typeof data[i] !== 'undefined' && data[i] !== null){

				(function(d, m, idx){
					calls.push(
						function(callback){
							self.convertDataRelationToDocument(metadata, idx, d, m, mapper, function(err, item) {
								callback(err, item);
							});
						}
					);
				}(data, model, i));
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){

			if (typeof data[i] !== 'undefined' && data[i] !== null){

				(function(d, m, idx){
					calls.push(
						function(callback){
							self.convertDataRelationToDocument(metadata, idx, d, m, mapper, function(err, item) {
								callback(err, item);
							});
						}
					);
				}(data, model, i));
			}
		}

		if (calls.length > 0){

			// run all queries and process data
			async.parallel(calls, function(err, document){
				cb(err, document);
			});

		} else {
			cb(null, model);
		}
	}
};

Mapper.prototype.constructor = Mapper;

module.exports = Mapper;