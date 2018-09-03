'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')

const CREATE_COLLECTION_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS collection_queries (
	collection_queries_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	"when" INT NOT NULL,
	duration INT NOT NULL,
	inputHash CHARACTER(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS collection_queries_created_idx ON collection_queries (created);
CREATE INDEX IF NOT EXISTS collection_queries_method_idx ON collection_queries (method);
CREATE INDEX IF NOT EXISTS collection_queries_when_idx ON collection_queries ("when");
CREATE INDEX IF NOT EXISTS collection_queries_duration_idx ON collection_queries (duration);
CREATE INDEX IF NOT EXISTS collection_queries_inputHash_idx ON collection_queries (inputHash);`

const CREATE_COLLECTIONS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS collections (
	collections_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	"when" INT,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES collection_queries(collection_queries_id)
);
CREATE INDEX IF NOT EXISTS collections_query_id_idx ON collections (query_id);`

const READ_COLLECTIONS = `\
SELECT collections.data FROM collection_queries
LEFT JOIN collections
	ON collection_queries.collection_queries_id = collections.query_id
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
	-- find queries that cover the when -> (when + duration) period
	AND collection_queries."when" <= $whenMin
	AND (collection_queries."when" + duration) >= $whenMax`

const WRITE_COLLECTION_QUERY = `\
INSERT OR REPLACE INTO collection_queries
(collection_queries_id, method, created, "when", duration, inputHash)
VALUES ($id, $method, $created, $when, $duration, $inputHash)`

const WRITE_COLLECTIONS = `\
INSERT INTO collections
(collections_id, query_id, "when", data)
VALUES ($id, $queryId, $when, $data)`

// "atomic queries": Queries whose return values can only be cached together.
// Example: Caching 1 of 3 journeys and reusing for other queries is impossible.

const CREATE_ATOMICS_TABLE = `\
CREATE TABLE IF NOT EXISTS atomics (
	atomics_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	inputHash CHARACTER(32) NOT NULL,
	data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS atomics_created_idx ON atomics (created);
CREATE INDEX IF NOT EXISTS atomics_inputHash_idx ON atomics (inputHash);
CREATE INDEX IF NOT EXISTS atomics_data_idx ON atomics (data);`

const READ_ATOMIC = `\
SELECT data FROM atomics
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
LIMIT 1`

const WRITE_ATOMIC = `\
INSERT OR REPLACE INTO atomics
(atomics_id, created, method, inputHash, data)
VALUES ($id, $created, $method, $inputHash, $data)`

const createStorage = (db) => {
	const init = (cb) => {
		debug('init')
		db.exec([
			CREATE_COLLECTION_QUERIES_TABLE,
			CREATE_COLLECTIONS_TABLE,
			CREATE_ATOMICS_TABLE
		].join('\n'), cb)
	}

	const readCollection = (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax,
			createdMin, createdMax
		} = args

		return new Promise((resolve, reject) => {
			db.all(READ_COLLECTIONS, {
				'$method': method, // 'dep' or 'arr'
				'$inputHash': inputHash,
				'$createdMin': Math.floor(createdMin / 1000),
				'$createdMax': Math.ceil(createdMax / 1000),
				'$whenMin': whenMin / 1000 | 0,
				'$whenMax': whenMax / 1000 | 0
			}, (err, rows) => {
				if (err) return reject(err)
				// todo: expose `.created`
				resolve(rows.map(row => JSON.parse(row.data)))
			})
		})
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash,
			when, duration,
			created, rows
		} = args
		const queryId = randomBytes(10).toString('hex')

		await new Promise((resolve, reject) => {
			db.run(WRITE_COLLECTION_QUERY, {
				'$id': queryId,
				'$method': method, // 'dep' or 'arr'
				'$created': created / 1000 | 0,
				'$when': when / 1000 | 0,
				'$duration': duration / 1000 | 0,
				'$inputHash': inputHash
			}, err => err ? reject(err) : resolve())
		})

		// todo: use `cmd = db.prepare; cmd.bind` for performance!
		// const cmd = db.prepare(WRITE_COLLECTIONS)
		for (let row of rows) {
			await new Promise((resolve, reject) => {
				db.run(WRITE_COLLECTIONS, {
					'$id': randomBytes(10).toString('hex'),
					'$queryId': queryId,
					'$when': new Date(row.when) / 1000 | 0, // todo
					'$data': row.data
				}, err => err ? reject(err) : resolve())
			})
		}
		// await new Promise((resolve, reject) => {
		// 	cmd.finalize(err => err ? reject(err) : resolve())
		// })
	}

	const readAtomic = (method, inputHash, createdMin, createdMax) => {
		debug('readAtomic', {method, inputHash, createdMin, createdMax})
		return new Promise((resolve, reject) => {
			db.get(READ_ATOMIC, {
				'$method': method,
				'$inputHash': inputHash,
				'$createdMin': Math.floor(createdMin / 1000),
				'$createdMax': Math.ceil(createdMax / 1000)
			}, (err, row) => {
				if (err) return reject(err)
				resolve(row && row.data ? JSON.parse(row.data) : null)
			})
		})
	}

	const writeAtomic = (method, inputHash, created, val) => {
		debug('writeAtomic', {method, inputHash, created, val})
		return new Promise((resolve, reject) => {
			db.run(WRITE_ATOMIC, {
				'$id': randomBytes(10).toString('hex'),
				'$created': created / 1000 | 0,
				'$method': method,
				'$inputHash': inputHash,
				'$data': JSON.stringify(val)
			}, (err) => {
				if (err) reject(err)
				else resolve()
			})
		})
	}

	return {
		init,
		readCollection,
		writeCollection,
		readAtomic,
		writeAtomic
	}
}

module.exports = createStorage
