<?php

/**
 * @see Zend_Cache_Backend
 */
require_once 'Zend/Cache/Backend.php';

/**
 * @see Zend_Cache_Backend_ExtendedInterface
 */
require_once 'Zend/Cache/Backend/ExtendedInterface.php';

/**
 *
 * Notes:
 * Use MongoDB 3.x
 *
 * @author     Andre Lohmann (lohmann.andre@gmail.com)
 * @package    Zend_Cache
 * @subpackage Zend_Cache_Backend_Mongodb
 * @copyright  Copyright (c) 2005 Andre Lohmann (https://github.com/andrelohmann)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */
class Zend_Cache_Backend_Mongodb extends Zend_Cache_Backend implements Zend_Cache_Backend_ExtendedInterface {

	const DEFAULT_HOST = '127.0.0.1';
	const DEFAULT_PORT = 27017;
	const DEFAULT_DBNAME = 'Db_Cache';
	const DEFAULT_COLLECTION = 'C_Cache';

	/**
	 *
	 * @var MongoDb|null
	 */
	protected $_database = null;

	/**
	 * MongoCollection
	 *
	 * @var MongoCollection|null $_collection
	 */
	protected $_collection = null;

	/**
	 * Available options:
	 * 'host' => (string): the name of the MongoDB server
	 * 'port' => (int): the port of the MongoDB server
	 * 'user' => (string): username to connect as
	 * 'password' => (string): password to connect with
	 * 'dbname' => (string): name of the database to use
	 * 'collection' => (string): name of the collection to use
	 *
	 * @var array
	 */
	protected $_options = array(
		'host' => self::DEFAULT_HOST,
		'port' => self::DEFAULT_PORT,
		'dbname' => self::DEFAULT_DBNAME,
		'collection' => self::DEFAULT_COLLECTION,
	);

	/**
	 * Note that we use TTL Collections to have the Mongo deamon automatically clean
	 * expired entries.
	 *
	 * @link http://docs.mongodb.org/manual/tutorial/expire-data/
	 * @param array $options
	 */
	public function __construct(array $options = array()) {
		if (!extension_loaded('mongo')) {
			Zend_Cache::throwException('The MongoDB extension must be loaded for using this backend !');
		}
		parent::__construct($options);

		// Merge the options passed in; overriding any default options
		$this->_options = array_merge($this->_options, $options);

		$conn = new \MongoClient($this->_getServerConnectionUrl());
		$this->_database = $conn->{$this->_options['dbname']};
		$this->_collection = $this->_database->selectCollection($this->_options['collection']);

		$this->_collection->createIndex(
				array('tags' => 1), array('background' => true)
		);
		$this->_collection->createIndex(
				array('expires_at' => 1), array(
			'background' => true,
			'expireAfterSeconds' => 0 // Have entries expire directly (0 seconds) after reaching expiration time
				)
		);
	}

	/**
	 * Assembles the URL that can be used to connect to the MongoDB server.
	 *
	 * Note that:
	 *  - FALSE, NULL or empty string values should be used to discard options
	 *    in an environment-specific configuration. For example when a 'development'
	 *    environment overrides a 'production' environment, it might be required
	 *    to discard the username and/or password, when this is not required
	 *    during development
	 *
	 * @link http://www.php.net/manual/en/mongoclient.construct.php
	 * @return string
	 */
	private function _getServerConnectionUrl() {
		$parts = array('mongodb://');
		if (isset($this->_options['username']) && strlen($this->_options['username']) > 0 && isset($this->_options['password']) && strlen($this->_options['password']) > 0) {
			$parts[] = $this->_options['username'];
			$parts[] = ':';
			$parts[] = $this->_options['password'];
			$parts[] = '@';
		}

		$parts[] = isset($this->_options['host']) && strlen($this->_options['host']) > 0 ? $this->_options['host'] : self::DEFAULT_HOST;
		$parts[] = ':';
		$parts[] = isset($this->_options['port']) && is_numeric($this->_options['port']) ? $this->_options['port'] : self::DEFAULT_PORT;
		$parts[] = '/';
		$parts[] = isset($this->_options['dbname']) && strlen($this->_options['dbname']) > 0 ? $this->_options['dbname'] : self::DEFAULT_DBNAME;

		return implode('', $parts);
	}

	/**
	 * Sets the frontend directives.
	 *
	 * @param  array $directives Assoc of directives
	 * @return void
	 */
	public function setDirectives($directives) {
		parent::setDirectives($directives);
		$lifetime = $this->getLifetime(false);
		if ($lifetime === null) {
			// #ZF-4614 : we tranform null to zero to get the maximal lifetime
			parent::setDirectives(array('lifetime' => 0));
		}
	}

	/**
	 * Test if a cache is available for the given id and (if yes) return it (false else)
	 *
	 * Note : return value is always "string" (unserialization is done by the core not by the backend)
	 *
	 * @param  string  $id                     Cache id
	 * @param  boolean $doNotTestCacheValidity If set to true, the cache validity won't be tested
	 * @return string|false cached datas
	 */
	public function load($id, $doNotTestCacheValidity = false) {
		try {
			if ($res = $this->_get($id)) {
				if ($doNotTestCacheValidity || $res['expires_at'] === null || $res['expires_at']->sec >= time()) {
					return $res['content'];
				}
				return false;
			}
		} catch (Exception $e) {
			$this->_log(__METHOD__ . ': ' . $e->getMessage());
			return false;
		}

		return false;
	}

	/**
	 * Test if a cache is available or not (for the given id)
	 *
	 * @param  string $id cache id
	 * @return mixed|false (a cache is not available) or "last modified" timestamp (int) of the available cache record
	 */
	public function test($id) {
		try {
			if ($res = $this->_get($id)) {
				return $res['created_at'];
			}
		} catch (Exception $e) {
			$this->_log(__METHOD__ . ': ' . $e->getMessage());
			return false;
		}

		return false;
	}

	/**
	 * Save some string datas into a cache record
	 *
	 * Note : $data is always "string" (serialization is done by the
	 * core not by the backend)
	 *
	 * @param  string $data            Datas to cache
	 * @param  string $id              Cache id
	 * @param  array $tags             Array of strings, the cache record will be tagged by each string entry
	 * @param  int   $specificLifetime If != false, set a specific lifetime for this cache record (null => infinite lifetime)
	 * @return boolean true if no problem
	 */
	public function save($data, $id, $tags = array(), $specificLifetime = false) {
		try {
			$lifetime = $this->getLifetime($specificLifetime);
			$result = $this->_set($id, $data, $lifetime, $tags);
		} catch (Exception $e) {
			$this->_log(__METHOD__ . ': ' . $e->getMessage());
			$result = false;
		}
		return (bool) $result;
	}

	/**
	 * Remove a cache record
	 *
	 * @param  string $id Cache id
	 * @return boolean True if no problem
	 */
	public function remove($id) {
		try {
			$result = $this->_collection->remove(array('_id' => $id));
		} catch (Exception $e) {
			$this->_log(__METHOD__ . ': ' . $e->getMessage());
			return false;
		}
		return $result;
	}

	/**
	 * Clean some cache records
	 *
	 * Available modes are :
	 * Zend_Cache::CLEANING_MODE_ALL (default)    => remove all cache entries ($tags is not used)
	 * Zend_Cache::CLEANING_MODE_OLD              => remove too old cache entries ($tags is not used)
	 * Zend_Cache::CLEANING_MODE_MATCHING_TAG     => remove cache entries matching all given tags
	 *                                               ($tags can be an array of strings or a single string)
	 * Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG => remove cache entries not {matching one of the given tags}
	 *                                               ($tags can be an array of strings or a single string)
	 * Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG => remove cache entries matching any given tags
	 *                                               ($tags can be an array of strings or a single string)
	 *
	 * @param  string $mode Clean mode
	 * @param  array  $tags Array of tags
	 * @return boolean true if no problem
	 */
	public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array()) {
		switch ($mode) {
			case Zend_Cache::CLEANING_MODE_ALL:
				return $this->_collection->remove(array());
			case Zend_Cache::CLEANING_MODE_OLD:
				return $this->_collection->remove(array('expires_at' => array('$lt' => new \MongoDate())));
			case Zend_Cache::CLEANING_MODE_MATCHING_TAG:
				return $this->_collection->remove(array('tags' => array('$all' => array_values($tags))));
			case Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
				return $this->_collection->remove(array('tags' => array('$nin' => array_values($tags))));
			case Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
				return $this->_collection->remove(array('tags' => array('$in' => array_values($tags))));
			default:
				Zend_Cache::throwException('Invalid mode for clean() method');
				return false;
		}
	}

	/**
	 * Return true if the automatic cleaning is available for the backend
	 *
	 * @return boolean
	 */
	public function isAutomaticCleaningAvailable() {
		return false;
	}

	/**
	 * Return an array of stored cache ids
	 *
	 * @return array array of stored cache ids (string)
	 */
	public function getIds() {
		$cursor = $this->_collection->find();
		$ret = array();
		while ($tmp = $cursor->getNext()) {
			$ret[] = $tmp['_id'];
		}
		return $ret;
	}

	/**
	 * Return an array of stored tags
	 *
	 * @return array array of stored tags (string)
	 */
	public function getTags() {
		$keys = array();
		$map = new MongoCode(
				'function() {
                for ( var key in this.tags) {
                    emit(this.tags[key], null);
                }
            }'
		);
		$reduce = new MongoCode(
				'function(key, tmp) {
				return null;
            }'
		);
		$result = $this->_database->command(array(
			"mapreduce" => $this->_options['collection'],
			"map" => $map,
			"reduce" => $reduce,
			"out" => array("inline" => true))
		);
		foreach ($result['results'] as $item) {
			$keys[] = $item['_id'];
		}
		return $keys;
	}

	/**
	 * Return an array of stored cache ids which match given tags
	 *
	 * In case of multiple tags, a logical AND is made between tags
	 *
	 * @param array $tags array of tags
	 * @return array array of matching cache ids (string)
	 */
	public function getIdsMatchingTags($tags = array()) {
		$cursor = $this->_collection->find(
				array('tags' => array('$all' => array_values($tags)))
		);
		$ret = array();
		while ($tmp = $cursor->getNext()) {
			$ret[] = $tmp['_id'];
		}
		return $ret;
	}

	/**
	 * Return an array of stored cache ids which don't match given tags
	 *
	 * In case of multiple tags, a logical OR is made between tags
	 *
	 * @param array $tags array of tags
	 * @return array array of not matching cache ids (string)
	 */
	public function getIdsNotMatchingTags($tags = array()) {
		$cursor = $this->_collection->find(
				array('tags' => array('$nin' => array_values($tags)))
		);
		$ret = array();
		while ($tmp = $cursor->getNext()) {
			$ret[] = $tmp['_id'];
		}
		return $ret;
	}

	/**
	 * Return an array of stored cache ids which match any given tags
	 *
	 * In case of multiple tags, a logical AND is made between tags
	 *
	 * @param array $tags array of tags
	 * @return array array of any matching cache ids (string)
	 */
	public function getIdsMatchingAnyTags($tags = array()) {
		$cursor = $this->_collection->find(
				array('tags' => array('$in' => array_values($tags)))
		);
		$ret = array();
		while ($tmp = $cursor->getNext()) {
			$ret[] = $tmp['_id'];
		}
		return $ret;
	}

	/**
	 * Return the filling percentage of the backend storage
	 *
	 * @return int integer between 0 and 100
	 */
	public function getFillingPercentage() {
		$result = $this->_database->execute('db.stats()');
		$total = @$result['retval']['storageSize'] ? : 0;
		$free = $total - @$result['retval']['dataSize'] ? : 0;
		if ($total == 0) {
			Zend_Cache::throwException('can\'t get disk_total_space');
		} else {
			if ($free >= $total) {
				return 100;
			}
			return ((int) (100. * ($total - $free) / $total));
		}
	}

	/**
	 * Return an array of metadatas for the given cache id
	 *
	 * The array must include these keys :
	 * - expire : the expire timestamp
	 * - tags : a string array of tags
	 * - mtime : timestamp of last modification time
	 *
	 * @param string $id cache id
	 * @return array array of metadatas (false if the cache id is not found)
	 */
	public function getMetadatas($id) {
		if ($tmp = $this->_get($id)) {
			$expiresAt = $tmp['expires_at'];
			$createdAt = $tmp['created_at'];
			return array(
				'expire' => $expiresAt instanceof \MongoDate ? $expiresAt->sec : null,
				'tags' => $tmp['tags'],
				'mtime' => $createdAt->sec
			);
		}
		return false;
	}

	/**
	 * Give (if possible) an extra lifetime to the given cache id
	 *
	 * @param string $id cache id
	 * @param int $extraLifetime
	 * @return boolean true if ok
	 */
	public function touch($id, $extraLifetime) {
		$result = false;
		if ($tmp = $this->_get($id)) {
			// Check whether an expiration time has been set that has not expired yet.
			if ($tmp['expires_at'] instanceof \MongoDate && $tmp['expires_at']->sec > time()) {
				$newLifetime = $tmp['expires_at']->sec + $extraLifetime;
				$result = $this->_set($id, $tmp['content'], $newLifetime, $tmp['tags']);
			}
		}
		return $result;
	}

	/**
	 * Return an associative array of capabilities (booleans) of the backend
	 *
	 * The array must include these keys :
	 * - automatic_cleaning (is automating cleaning necessary)
	 * - tags (are tags supported)
	 * - expired_read (is it possible to read expired cache records
	 *                 (for doNotTestCacheValidity option for example))
	 * - priority does the backend deal with priority when saving
	 * - infinite_lifetime (is infinite lifetime can work with this backend)
	 * - get_list (is it possible to get the list of cache ids and the complete list of tags)
	 *
	 * @return array associative of with capabilities
	 */
	public function getCapabilities() {
		return array(
			'automatic_cleaning' => true,
			'tags' => true,
			'expired_read' => true,
			'priority' => false,
			'infinite_lifetime' => true,
			'get_list' => true
		);
	}

	/**
	 * Saves data to a the MongoDB collection.
	 *
	 * @param integer $id
	 * @param array $data
	 * @param integer $lifetime
	 * @param mixed $tags
	 * @return boolean
	 */
	private function _set($id, $data, $lifetime, $tags) {
		list ($nowMicroseconds, $nowSeconds) = explode(' ', microtime());
		$nowMicroseconds = intval($nowMicroseconds * 1000000); // convert from 'expressed in seconds' to complete microseconds
		return $this->_collection->save(
						array(
							'_id' => $id,
							'content' => $data,
							'created_at' => new \MongoDate($nowSeconds, $nowMicroseconds),
							'expires_at' => is_numeric($lifetime) && intval($lifetime) !== 0 ? new \MongoDate($nowSeconds + $lifetime, $nowMicroseconds) : null,
							'tags' => array_values($tags)
						)
		);
	}

	/**
	 * Lookups a specific cache entry.
	 *
	 * Optionally, increment the hit counter when loading the cache entry
	 * (this increases load on the master, so by default it is turned off).
	 *
	 * @param integer $id
	 * @param boolean $incrementHitCounter = false
	 * @return array|bool
	 */
	private function _get($id) {
		return $this->_collection->findOne(array('_id' => $id));
	}

}
