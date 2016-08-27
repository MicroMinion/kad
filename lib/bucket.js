'use strict';

var _ = require('lodash');
var assert = require('assert');
var constants = require('./constants');
var Contact = require('./contact');
var async = require('async');

/**
* A bucket is a "column" of the routing table. It is an array-like object that
* holds {@link Contact}s.
* @constructor
*/
function Bucket(index, storage) {
  if (!(this instanceof Bucket)) {
    return new Bucket();
  }
  assert(_.isNumber(index) && index >= 0 && index <= constants.B);
  this.index = index;
  this.contacts = [];
  this._cache = {};
  this._setStorageAdapter(storage);
}

/**
 * Return the number of contacts in this bucket
 * @returns {Number}
 */
Bucket.prototype.getSize = function() {
  return this.contacts.length;
};

/**
 * Return the list of contacts in this bucket
 * @returns {Array}
 */
Bucket.prototype.getContactList = function() {
  return _.clone(_.values(this._cache));
};

/**
 * Return the contact at the given index
 * @param {Number} index - Index of contact in bucket
 * @returns {Contact|null}
 */
Bucket.prototype.getContact = function(index, callback) {
  assert(index >= 0 && index <= constants.B, 'Invalid index');
  assert(callback instanceof Function);
  if(this.contacts.length < index) {
    callback(new Error('Invalid contact index'));
  } else {
    this._storage.get(this.contacts[index], callback);
  }
};

/**
 * Adds the contact to the bucket
 * @param {Contact} contact - Contact instance to add to bucket
 * @returns {Boolean} added - Indicates whether or not the contact was added
 */
Bucket.prototype.addContact = function(contact, callback) {
  assert(contact instanceof Contact, 'Invalid contact supplied');
  assert(callback instanceof Function, 'No callback supplied');
  var self = this;
  if (this.getSize() === constants.K) {
    return callback(new Error('Bucket full'));
  }
  this._cache[contact.nodeID] = contact;
  if (!_.includes(self.contacts, contact.nodeID)) {
    var idx = _.sortedIndex(self.contacts, contact.nodeID, function(nodeID) {
      return self._cache[nodeID].lastSeen;
    });
    self.contacts.splice(idx, 0, contact.nodeID);
    callback(null, contact);
  } else {
    callback(new Error('Contact already in bucket'));
  }
};

/**
 * Removes the contact from the bucket
 * @param {Contact} contact - Contact instance to remove from bucket
 * @returns {Boolean} removed - Indicates whether or not the contact was removed
 */
Bucket.prototype.removeContact = function(contact, callback) {
  assert(contact instanceof Contact, 'Invalid contact supplied');
  assert(callback instanceof Function, 'No callback supplied');
  var index = _.indexOf(this.contacts, contact.nodeID);

  if (index >= 0) {
    this.contacts.splice(index, 1);
    callback(null, contact);
  }
  callback(new Error('Contact not in bucket'));
};

/**
 * Returns boolean indicating that the nodeID is contained in the bucket
 * @param {String} nodeID - 160 bit node ID
 * @returns {Boolean}
 */
Bucket.prototype.hasContact = function(nodeID) {
  return _.includes(this.contacts, nodeID);
};

/**
 * Returns the index of the given contact
 * @param {Contact} contact - Contact instance for index check
 * @returns {Number}
 */
Bucket.prototype.indexOf = function(contact) {
  assert(contact instanceof Contact, 'Invalid contact supplied');
  return _.indexOf(this.contacts, contact.nodeID);
};

Bucket.prototype.save = function(callback) {
  assert(callback instanceof Function, 'No callback supplied');
  var self = this;
  async.series([
    this._storage.put.bind(this._storage, 'BUCKET-' + this.index,
                          this.contacts),
    function(cb) {
        self._storage.get('BUCKETS', function(err, results) {
          if(err || !results) {
            results = [];
          }
          var index = _.indexOf(results, self.index);
          if(index < 0) {
            results.push(self.index);
          }
          self._storage.put('BUCKETS', results, cb);
        });
    }
  ], function(err) {
    if(err) {
      callback(err);
    } else {
      callback();
    }
  });
};

Bucket.prototype.load = function(callback) {
  assert(callback instanceof Function, 'No callback supplied');
  var self = this;
  this._storage.get('BUCKET-' + this.index, function(err, contacts) {
    if(!err && contacts) {
      self.contacts = contacts;
    }
    callback();
  });
};

Bucket.prototype.empty = function(callback) {
  assert(callback instanceof Function, 'No callback supplied');
  var self = this;
  async.series([
    self.load.bind(self),
    function(cb) {
      async.each(self.contacts, self._storage.del.bind(self._storage), cb);
    },
    self._storage.del.bind(self._storage, 'BUCKET-' + self.index)
  ], callback);
};

Bucket.prototype.loadContacts = function(callback) {
  assert(callback instanceof Function, 'No callback supplied');
  var self = this;
  self._cache = {};
  async.each(self.contacts, function(item, cb) {
    self._storage.get(item, function(err, result) {
      if(err || !result) {
        cb(err);
      } else {
        self._cache[result.nodeID] = result;
        cb();
      }
    });
  }, callback);
};

/**
 * Validates the set storage adapter
 * @private
 * @param {Object} storage
 */
Bucket.prototype._setStorageAdapter = function(storage) {
  assert(typeof storage === 'object', 'No storage adapter supplied');
  assert(typeof storage.get === 'function', 'Store has no get method');
  assert(typeof storage.put === 'function', 'Store has no put method');
  assert(typeof storage.del === 'function', 'Store has no del method');
  assert(
    typeof storage.createReadStream === 'function',
    'Store has no createReadStream method'
  );

  this._storage = storage;
};


module.exports = Bucket;
