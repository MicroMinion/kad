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
function Bucket(index, routingTable) {
  if (!(this instanceof Bucket)) {
    return new Bucket();
  }
  assert(_.isNumber(index) && index >= 0 && index <= constants.B);
  this.index = index;
  this.contacts = [];
  this._cache = {};
  this._routingTable = routingTable;
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
    return callback(new Error('Invalid contact index'));
  } else {
    this._routingTable.getContact(this.contacts[index], callback);
  }
};

Bucket.prototype.getContactSync = function(index) {
  assert(index >= 0 && index <= constants.B, 'Invalid index');
  assert(index < this.contacts.length);
  assert(_.has(this._cache, this.contacts[index]));
  var nodeID = this.contacts[index];
  return this._cache[nodeID];
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
    return callback(null, contact);
  } else {
    return callback(new Error('Contact already in bucket'));
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
    return callback(null, contact);
  } else {
    return callback(new Error('Contact not in bucket'));
  }
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
  this._routingTable.save(callback);
};


Bucket.prototype.loadContacts = function(callback) {
  var self = this;
  assert(callback instanceof Function, 'No callback supplied');
  async.each(this.contacts, function(nodeID, cb) {
      self._routingTable.getContact(nodeID, function(err, contact) {
        if(err) {
          return cb(err);
        } else {
          self._cache[nodeID] = contact;
          return cb();
        }
      });
  }, function(err) {
    if(err) {
      return callback(err);
    } else {
      return callback(null, self);
    }
  });
};

module.exports = Bucket;
