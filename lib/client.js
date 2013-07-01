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

	insert: function(metadata, collection, data, cb){
		this.db.collection(collection, function(err, coll) {
			coll.insert(data, function(err, docs){
				data[metadata.getIdFieldName()] = docs[0][metadata.getIdFieldName()];
				cb(err, data);
			});
		}); 
	},

	update: function(metadata, collection, id, data, cb){

		console.log("UPDATE");

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

	remove: function(metadata, collection, id, cb){

		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.remove(cond, 1, function(err){
				cb(err);
			});
		});
	},

	find: function(metadata, collection, id, cb){
		this.db.collection(collection, function(err, coll){

			var cond = {};
			cond[metadata.getIdFieldName()] = id;

			coll.findOne(cond, function(err, item){
				cb(err, item);
			});
		});
	},

	findBy: function(metadata, collection, criteria, sort, skip, limit, cb){

		if(typeof criteria === 'undefined'){
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
	}
};

module.exports = Client;