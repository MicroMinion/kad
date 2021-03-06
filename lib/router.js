'use strict'

var assert = require('assert')
var inherits = require('util').inherits
var events = require('events')
var async = require('async')
var _ = require('lodash')
var constants = require('./constants')
var utils = require('./utils')
var Message = require('./message')
var Logger = require('./logger')
var Contact = require('./contact')
var Item = require('./item')
var RoutingTable = require('./routing-table')

/**
 * Represents a routing table of known {@link Contact}s; used by {@link Node}.
 * @constructor
 * @param {Object} options
 * @param {Logger} options.logger - Logger instance to use
 * @param {RPC} options.transport - Transport adapter (RPC) to use
 * @param {Router~validator} options.validator - Key-Value validation function
 * @emits Router#add
 * @emits Router#drop
 * @emits Router#shift
 */
function Router (options) {
  if (!(this instanceof Router)) {
    return new Router(options)
  }

  this._log = options.logger || new Logger(4)
  // this._log = new Logger(4)
  this._rpc = options.transport
  this._self = this._rpc._contact
  this._validator = options.validator
  this._routingTable = new RoutingTable(options.storage, this._rpc)
}
/**
 * Called when a value is returned from a lookup to validate it
 * @callback Router~validator
 * @param {String} key - The key at which the value is stored
 * @param {String} value - The returned value from the lookup
 * @param {Function} callback - Called with boolean indicating validity
 */

/**
 * Add event is triggered when a new {@link Contact} is added to the router
 * @event Router#add
 */

/**
 * Drop event is triggered when a {@link Contact} is dropped from the router
 * @event Router#drop
 */

/**
 * Shift event is triggered when a {@link Contact} changes position in a
 * {@link Bucket}
 * @event Router#shift
 */

inherits(Router, events.EventEmitter)

/**
 * Execute this router's find operation with the shortlist
 * @param {String} type - One of "NODE" or "VALUE"
 * @param {String} key - Key to use for lookup
 * @param {Router~lookupCallback} callback - Called when the lookup is complete
 */
Router.prototype.lookup = function (type, key, callback) {
  assert(['NODE', 'VALUE'].indexOf(type) !== -1, 'Invalid search type')
  var self = this
  this._createLookupState(type, key, function (err, state) {
    if (!state.closestNode) {
      return callback(new Error('Not connected to any peers'))
    }
    state.closestNodeDistance = utils.getDistance(
      state.hashedKey,
      state.closestNode.nodeID
    )
    self._log.debug('performing network walk', {
      type: type,
      key: key
    })
    self._iterativeFind(state, state.shortlist, callback)
  })
}
/**
 * This callback is called upon completion of {@link Router#lookup}
 * @callback Router~lookupCallback
 * @param {Error|null} err - The error object, if any
 * @param {String} type - One of "NODE" or "VALUE"
 * @param {Array|String} result - The {@link Contact}s returned or the value
 */

/**
 * Returns the number of contacts in the routing table - used as the getter for
 * the Router#length property
 * @private
 */
Router.prototype.getSize = function (callback) {
  this._routingTable.getSize(callback)
}

/**
 * Empties the routing table, clearing all known contacts
 */
Router.prototype.empty = function (callback) {
  this._routingTable.empty(callback)
}

Router.prototype.isConnected = function (contact, callback) {
  this._routingTable.inTable(contact, function (err) {
    if (err) {
      return callback()
    } else {
      return callback(new Error('Contact already in routing table'))
    }
  })
}

/**
 * Removes the given contact from the routing table
 * @param {Contact} contact - The contact to drop from the router
 * @param {Function} callback - Contact was in expected bucket
 */
Router.prototype.removeContact = function (contact, callback) {
  var self = this
  var index = utils.getBucketIndex(this._self.nodeID, contact.nodeID)
  this._log.debug('removing contact', {
    contact: contact
  })
  assert(index < constants.B, 'Bucket index may not exceed B')
  async.waterfall([
    self._routingTable.getBucket.bind(self._routingTable, index),
    function (bucket, removeCb) {
      bucket.removeContact(contact, function (err, contact) {
        return removeCb(err, bucket, contact)
      })
    },
    function (bucket, contact, saveCb) {
      bucket.save(saveCb)
    },
    function (dropCb) {
      self.emit('drop', contact)
      return dropCb()
    }
  ], callback)
}

/**
 * Returns the {@link Contact} in the routing table by the given node ID
 * @param {String} nodeID - The nodeID of the {@link Contact} to return
 */
Router.prototype.getContactByNodeID = function (nodeID, callback) {
  var self = this
  var lookup = function (nodeID, lookupCb) {
    self.lookup('NODE', nodeID, function (err, type, shortlist) {
      if (err) {
        return lookupCb(err)
      } else {
        var result = _.find(shortlist, function (contact) {
          return nodeID === contact.nodeID
        })
        if (result) {
          return lookupCb(null, result)
        } else {
          return lookupCb(new Error('Contact not found'))
        }
      }
    })
  }
  this._routingTable.getContact(nodeID, function (err, result) {
    if (err) {
      return lookup(nodeID, callback)
    } else {
      return callback(null, result)
    }
  })
}

/**
 * Creates a state machine for a lookup operation
 * @private
 * @param {String} type - One of 'NODE' or 'VALUE'
 * @param {String} key - 160 bit key
 * @returns {Object} Lookup state machine
 */
Router.prototype._createLookupState = function (type, key, callback) {
  var state = {
    type: type,
    key: key,
    hashedKey: utils.createID(key),
    limit: constants.ALPHA,
    previousClosestNode: null,
    contacted: {},
    foundValue: false,
    value: null,
    contactsWithoutValue: []
  }
  this.getNearestContacts(
    key,
    state.limit,
    this._self.nodeID,
    function (err, shortlist) {
      state.shortlist = shortlist
      state.closestNode = state.shortlist[0]
      return callback(err, state)
    }
  )
}

/**
 * Execute the find operation for this router type
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Array} contacts - List of contacts to query
 * @param {Function} callback
 */
Router.prototype._iterativeFind = function (state, contacts, callback) {
  var self = this
  var failures = 0

  function queryContact (contact, next) {
    self._queryContact(state, contact, function (err) {
      if (err) {
        failures++
      }
      return next()
    })
  }

  this._log.debug('starting contact iteration', {
    key: state.key
  })
  async.each(contacts, queryContact, function () {
    self._log.debug('finished iteration, handling results')

    if (failures === contacts.length) {
      return callback(new Error('Lookup operation failed to return results'))
    }

    self._handleQueryResults(state, callback)
  })
}

/**
 * Send this router's RPC message to the contact
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Contact} contact - Contact to query
 * @param {Function} callback
 */
Router.prototype._queryContact = function (state, contactInfo, callback) {
  var self = this
  var contact = this._rpc._createContact(contactInfo)
  var message = new Message({
    method: 'FIND_' + state.type,
    params: {
      key: state.key,
      contact: this._self
    }
  })

  this._log.debug('querying', {
    nodeID: contact.nodeID,
    key: state.key
  })
  this._rpc.send(contact, message, function (err, response) {
    if (err) {
      self._log.warn(
        'query failed, removing contact for shortlist', {
          error: err.message
        }
      )
      self._removeFromShortList(state, contact.nodeID)
      self.removeContact(contact)
      return callback(err)
    }

    self._handleFindResult(state, response, contact, callback)
  })
}

/**
 * Handle the results of the contact query
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Message} message - Received response to FIND_* RPC
 * @param {Contact} contact - Sender of the message
 * @param {Function} callback
 */
Router.prototype._handleFindResult = function (state, msg, contact, callback) {
  var distance = utils.getDistance(state.hashedKey, contact.nodeID)

  state.contacted[contact.nodeID] = this.updateContact(contact)

  if (utils.compareKeys(distance, state.closestNodeDistance) === -1) {
    state.previousClosestNode = state.closestNode
    state.closestNode = contact
    state.closestNodeDistance = distance
  }

  if (state.type === 'NODE') {
    this._addToShortList(state, msg.result.nodes)
    return callback()
  }

  if (!msg.result.item) {
    state.contactsWithoutValue.push(contact)
    this._addToShortList(state, msg.result.nodes)
    return callback()
  }

  this._validateFindResult(state, msg, contact, callback)
}

/**
 * Validates the data returned from a find
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Message} message - Received response to FIND_* RPC
 * @param {Contact} contact - Sender of the message
 * @param {Function} callback
 */
Router.prototype._validateFindResult = function (state, msg, contact, done) {
  var self = this
  var item = msg.result.item

  function rejectContact () {
    self._removeFromShortList(state, contact.nodeID)
    self.removeContact(contact)
    return done()
  }

  this._log.debug('validating result', {
    sender: contact.nodeID
  })
  this._validateKeyValuePair(state.key, item.value, function (valid) {
    if (!valid) {
      self._log.warn('failed to validate key/value pair', {
        key: state.key
      })
      return rejectContact()
    }

    state.foundValue = true
    state.value = item.value
    state.item = item
    return done()
  })
}

/**
 * Add contacts to the shortlist, preserving nodeID uniqueness
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Array} contacts - Contacts to add to the shortlist
 */
Router.prototype._addToShortList = function (state, contacts) {
  assert(Array.isArray(contacts), 'No contacts supplied')
  state.shortlist = state.shortlist.concat(contacts)
  state.shortlist = _.uniq(state.shortlist, false, 'nodeID')
}

/**
 * Remove contacts with the nodeID from the shortlist
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {String} nodeID - Node ID of the contact to remove
 */
Router.prototype._removeFromShortList = function (state, nodeID) {
  state.shortlist = _.reject(state.shortlist, function (c) {
    return c.nodeID === nodeID
  })
}

/**
 * Handle the results of all the executed queries
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Function} callback
 */
Router.prototype._handleQueryResults = function (state, callback) {
  if (state.foundValue) {
    this._log.debug('a value was returned', {
      key: state.key
    })
    return this._handleValueReturned(state, callback)
  }

  var closestNodeUnchanged = state.closestNode === state.previousClosestNode
  var shortlistFull = state.shortlist.length >= constants.K

  if (closestNodeUnchanged || shortlistFull) {
    this._log.debug(
      'shortlist is full or there are no known nodes closer to key', {
        key: state.key
      }
    )
    return callback(null, 'NODE', state.shortlist)
  }

  var remainingContacts = _.reject(state.shortlist, function (c) {
    return state.contacted[c.nodeID]
  })

  if (remainingContacts.length === 0) {
    this._log.debug('there are no more remaining contacts to query')
    return callback(null, 'NODE', state.shortlist)
  }

  this._log.debug('continuing with iterative query', {
    key: state.key
  })
  this._iterativeFind(
    state,
    remainingContacts.splice(0, constants.ALPHA),
    callback
  )
}

/**
 * Handle a value being returned and store at closest nodes that didn't have it
 * @private
 * @param {Object} state - State machine returned from _createLookupState()
 * @param {Function} callback
 */
Router.prototype._handleValueReturned = function (state, callback) {
  var self = this

  var distances = state.contactsWithoutValue.map(function (contact) {
    return {
      distance: utils.getDistance(contact.nodeID, self._self.nodeID),
      contact: contact
    }
  })

  distances.sort(function (a, b) {
    return utils.compareKeys(a.distance, b.distance)
  })

  if (distances.length >= 1) {
    var item = state.item
    var closestWithoutValue = distances[0].contact
    var message = new Message({
      method: 'STORE',
      params: {
        item: new Item(item.key, item.value, item.publisher, item.timestamp),
        contact: this._self
      }
    })

    this._rpc.send(closestWithoutValue, message)
  }

  return callback(null, 'VALUE', state.value)
}

/**
 * Refreshes the buckets farther than the closest known
 * @param {Array} contacts - Results returned from findNode()
 * @param {Router~refreshBeyondCallback} done - Called upon successful refresh
 */
Router.prototype.refreshBucketsBeyondClosest = function (contacts, done) {
  var self = this
  async.waterfall([
    self._routingTable.getIndexes.bind(self._routingTable),
    function (bucketIndexes, cb) {
      var leastBucket = _.min(bucketIndexes)
      var refreshBuckets = _.filter(bucketIndexes, function (index) {
        return index > leastBucket
      })
      var doneCount = 0
      _.forEach(refreshBuckets, function (index) {
        self.refreshBucket(index, function () {
          doneCount++
          if (doneCount === refreshBuckets.length) {
            cb()
          }
        })
      })
    }
  ], function () {
    return done()
  })
}
/**
 * This callback is called upon completion of
 * {@link Router#refreshBucketsBeyondClosest}
 * @callback Router~refreshBeyondCallback
 */

/**
 * Refreshes the bucket at the given index
 * @param {Number} index
 * @param {Function} callback
 */
Router.prototype.refreshBucket = function (index, callback) {
  var random = utils.getRandomInBucketRangeBuffer(index)
  this.findNode(random.toString('hex'), callback)
}
/**
 * This callback is called upon completion of {@link Router#refreshBucket}
 * @callback Router~refreshCallback
 * @param {Error|null} err - The error object, if any
 * @param {Array} contacts - The list of {@link Contact}s refreshed
 */

/**
 * Search contacts for the value at given key
 * @param {String} key - The lookup key for the desired value
 * @param {Router~findValueCallback} callback - Called upon lookup completion
 */
Router.prototype.findValue = function (key, callback) {
  var self = this

  this._log.debug('searching for value', {
    key: key
  })

  this.lookup('VALUE', key, function (err, type, value) {
    if (err || type === 'NODE') {
      return callback(new Error('Failed to find value for key: ' + key))
    }

    self._log.debug('found value', {
      key: key
    })

    return callback(null, value)
  })
}
/**
 * This callback is called upon completion of {@link Router#findValue}
 * @callback Router~findValueCallback
 * @param {Error|null} err - The error object, if any
 * @param {String} value - The value returned from the lookup
 */

/**
 * Search contacts for nodes close to the given key
 * @param {String} nodeID - The nodeID the search for neighbors
 * @param {Router~findNodeCallback} callback - Called upon lookup completion
 */
Router.prototype.findNode = function (nodeID, callback) {
  var self = this
  if (!callback) {
    callback = function () {}
  }
  this._log.debug('searching for nodes close', {
    key: nodeID
  })

  this.lookup('NODE', nodeID, function (err, type, contacts) {
    if (err) {
      return callback(err)
    }

    self._log.debug('found nodes close to key', {
      amount: contacts.length,
      key: nodeID
    })
    return callback(null, contacts)
  })
}
/**
 * This callback is called upon completion of {@link Router#findNode}
 * @callback Router~findNodeCallback
 * @param {Error|null} err - The error object, if any
 * @param {Array} value - The {@link Contact}s returned from the lookup
 */

/**
 * Update the contact's status
 * @param {Contact} contact - Contact to update
 * @param {Router~updateContactCallback} callback - Optional completion calback
 * @returns {Contact}
 */
Router.prototype.updateContact = function (contact, callback) {
  var self = this
  if (!callback) {
    callback = function () {}
  }
  var bucketIndex = utils.getBucketIndex(this._self.nodeID, contact.nodeID)
  this._log.debug('updating contact', {
    contact: contact
  })
  assert(bucketIndex < constants.B, 'Bucket index cannot exceed B')
  contact.seen()
  async.waterfall([
    self._routingTable.setContact.bind(self._routingTable, contact),
    self._routingTable.getBucket.bind(self._routingTable, bucketIndex),
    function (bucket, cb) {
      bucket.loadContacts(cb)
    }
  ], function (err, bucket) {
    if (bucket.hasContact(contact.nodeID)) {
      self._moveContactToTail(contact, bucket, callback)
    } else if (bucket.getSize() < constants.K) {
      self._moveContactToHead(contact, bucket, callback)
    } else {
      self._pingContactAtHead(contact, bucket, callback)
    }
  })
  return contact
}
/**
 * This callback is called upon completion of {@link Router#updateContact}
 * @callback Router~updateContactCallback
 */

/**
 * Move the contact to the bucket's tail
 * @private
 * @param {Contact} contact
 * @param {Bucket} bucket
 * @param {Function} callback
 */
Router.prototype._moveContactToTail = function (contact, bucket, callback) {
  var self = this
  if (!callback) {
    callback = function () {}
  }
  this._log.debug('contact already in bucket, moving to tail')
  async.waterfall([
    bucket.removeContact.bind(bucket, contact),
    bucket.addContact.bind(bucket),
    self._routingTable.setContact.bind(self._routingTable),
    bucket.save.bind(bucket),
    function (cb) {
      self.emit('shift', contact, bucket.index, bucket.indexOf(contact))
      return cb()
    }
  ], function (err, result) {
    return callback(err, result)
  })
}

/**
 * Move the contact to the bucket's head
 * @private
 * @param {Contact} contact
 * @param {Bucket} bucket
 * @param {Function} callback
 */
Router.prototype._moveContactToHead = function (contact, bucket, callback) {
  var self = this
  this._log.debug('contact not in bucket, moving to head')
  if (!callback) {
    callback = function () {}
  }
  async.waterfall([
    bucket.addContact.bind(bucket, contact),
    self._routingTable.setContact.bind(self._routingTable),
    bucket.save.bind(bucket),
    function (cb) {
      self.emit('add', contact, bucket.index, bucket.indexOf(contact))
      return cb()
    }
  ], callback)
}

/**
 * Ping the contact at head and if no response, replace with contact
 * @private
 * @param {Contact} contact
 * @param {Bucket} bucket
 * @param {Function} callback
 */
Router.prototype._pingContactAtHead = function (contact, bucket, callback) {
  var self = this
  var ping = new Message({
    method: 'PING',
    params: {
      contact: this._self
    }
  })
  var head = bucket.getContactSync(0)

  this._log.debug('no room in bucket, sending PING to contact at head')
  this._rpc.send(head, ping, function (err) {
    if (err) {
      self._log.debug('head contact did not respond, replacing with new')
      // NB: It's possible that the head contact has changed between pings
      // NB: timeout, so we need to make sure that we get the *most* stale
      // NB: contact.
      async.waterfall([
        bucket.getContact.bind(bucket, 0),
        bucket.removeContact.bind(bucket),
        function (contact, dropCb) {
          self.emit('drop', contact)
          return dropCb()
        },
        bucket.loadContacts.bind(bucket),
        function (bucket, addCb) {
          bucket.addContact(contact, addCb)
        },
        function (contact, emitAddCb) {
          self.emit('add', contact, bucket.index, bucket.indexOf(contact))
          return emitAddCb()
        }
      ], callback)
    } else {
      async.series([
        bucket.loadContacts.bind(bucket),
        bucket.removeContact.bind(bucket, head),
        bucket.addContact.bind(bucket)
      ], callback)
    }
  })
}

/**
 * Return contacts closest to the given key
 * @param {String} key - Lookup key for getting close {@link Contact}s
 * @param {Number} limit - Maximum number of contacts to return
 * @param {String} nodeID - Node ID to exclude from results
 * @returns {Array}
 */
Router.prototype.getNearestContacts = function (key, limit, nodeID, callback) {
  var self = this
  var contacts = []
  var index = utils.getBucketIndex(this._self.nodeID, utils.createID(key))
  var ascBucketIndex = index
  var descBucketIndex = index

  function addNearestFromBucket (bucket) {
    self._getNearestFromBucket(
      bucket,
      utils.createID(key),
      limit - contacts.length
    ).forEach(function addToContacts (contact) {
      var isContact = contact instanceof Contact
      var poolNotFull = contacts.length < limit
      var notRequester = contact.nodeID !== nodeID

      if (isContact && poolNotFull && notRequester) {
        contacts.push(contact)
      }
    })
  }

  function createBucket (index, createBucketCallback) {
    async.waterfall([
      self._routingTable.hasBucket.bind(self._routingTable, index),
      self._routingTable.getBucket.bind(self._routingTable, index),
      function (bucket, loadContactsCb) {
        bucket.loadContacts(loadContactsCb)
      },
      function (loadedBucket, addNearestCallback) {
        addNearestFromBucket(loadedBucket)
        return addNearestCallback()
      }
    ], function () {
      return createBucketCallback()
    })
  }
  async.series([
    createBucket.bind(null, index),
    function (ascCallback) {
      async.whilst(
        function () {
          return contacts.length < limit && ascBucketIndex < constants.B - 1
        },
        function (cb) {
          ascBucketIndex++
          createBucket(ascBucketIndex, cb)
        },
        ascCallback
      )
    },
    function (descCallback) {
      async.whilst(
        function () {
          return contacts.length < limit && descBucketIndex > 0
        },
        function (cb) {
          descBucketIndex--
          createBucket(descBucketIndex, cb)
        },
        descCallback
      )
    }
  ], function (err) {
    if (err) {
      return callback(err)
    } else {
      return callback(null, contacts)
    }
  })
}

/**
 * Get the contacts closest to the key from the given bucket
 * @private
 * @param {Bucket} bucket
 * @param {String} key
 * @param {Number} limit
 * @returns {Array}
 */
Router.prototype._getNearestFromBucket = function (bucket, key, limit) {
  if (!bucket) {
    return []
  }

  var nearest = bucket.getContactList().map(function addDistance (contact) {
    return {
      contact: contact,
      distance: utils.getDistance(contact.nodeID, key)
    }
  }).sort(function sortKeysByDistance (a, b) {
    return utils.compareKeys(a.distance, b.distance)
  }).splice(0, limit).map(function pluckContact (c) {
    return c.contact
  })

  return nearest
}

/**
 * Validates a key/value pair (defaults to true)
 * @private
 * @param {String} key
 * @param {String} value
 * @param {Function} callback
 */
Router.prototype._validateKeyValuePair = function (key, value, callback) {
  if (typeof this._validator === 'function') {
    return this._validator(key, value, callback)
  }

  return callback(true)
}

module.exports = Router
