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

var Mapper = function(registry, client){
	this.registry = registry;
	this.client = client;
};

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

		var converted = value;

		if (value instanceof ObjectID){
			converted = value.toHexString();
		}

		return converted;
	},

	convertRelationsToData: function(metadata, model, data){

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
	},

	convertDataRelationsToDocument: function(metadata, data, model, mapper, cb){

		var i;
		var self = this;
		var calls = [];

		// one-to-one
		for (i in metadata.relations['one-to-one']){

			if(typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, i){

					calls.push(

						function(callback){

							var dbRef = new DBRef(data[i].namespace, new ObjectID(data[i].oid));

							self.client.db.dereference(dbRef, function(err, item){
								model[i] = item;
								callback(model);
							});
						}
					);

				}(data, model, i));				
			}
		}

		// one-to-many
		for (i in metadata.relations['one-to-many']){

			if(typeof data[i] !== 'undefined' && data[i] !== null){

				(function(data, model, i){

					calls.push(

						function(callback){

							var ids = [];
							var relation = metadata.getRelationByFieldName(i);
							var relationMetadata = self.registry.getMetadataByName(relation.document);

							for(var j in data[i]){
								ids.push(new ObjectID(data[i][j].oid));
							}

							self.client.findBy(relationMetadata,
											   relationMetadata.collection, 
											   { "_id" : { "$in" : ids }},
											   null,
											   null,
											   null,
											   function(err, docs){
											   		model[i] = docs;
											   		callback(model);
											   }
							);
						}
					);

				}(data, model, i));				
			}
		}

		if (calls.length > 0){

			// run all queries and process data
			async.parallel(calls, function(document){
				cb(null, document);
			});

		} else {
			cb(null, model);
		}
	}
};

module.exports = Mapper;