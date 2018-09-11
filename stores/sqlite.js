'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')

const V = '1'

const CREATE_COLLECTION_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS collection_queries_${V} (
	collection_queries_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	"when" INT NOT NULL,
	duration INT NOT NULL,
	inputHash CHARACTER(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_created_idx
	ON collection_queries_${V} (created);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_method_idx
	ON collection_queries_${V} (method);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_when_idx
	ON collection_queries_${V} ("when");
CREATE INDEX IF NOT EXISTS collection_queries_${V}_duration_idx
	ON collection_queries_${V} (duration);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_inputHash_idx
	ON collection_queries_${V} (inputHash);`

const CREATE_COLLECTIONS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS collections_${V} (
	collections_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	"when" INT,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES collection_queries_${V}(collection_queries_id)
);
CREATE INDEX IF NOT EXISTS collections_${V}_query_id_idx
	ON collections_${V} (query_id);`

const READ_COLLECTIONS = `\
SELECT collections_${V}.data FROM collection_queries_${V}
LEFT JOIN collections_${V}
	ON collection_queries_${V}.collection_queries_id = collections_${V}.query_id
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
	-- find queries that cover the when -> (when + duration) period
	AND collection_queries_${V}."when" <= $whenMin
	AND (collection_queries_${V}."when" + duration) >= $whenMax`

const WRITE_COLLECTION_QUERY = `\
INSERT OR REPLACE INTO collection_queries_${V}
(collection_queries_id, method, created, "when", duration, inputHash)
VALUES ($id, $method, $created, $when, $duration, $inputHash)`

const WRITE_COLLECTIONS = `\
INSERT INTO collections_${V}
(collections_id, query_id, "when", data)
VALUES ($id, $queryId, $when, $data)`

// "atom queries": Queries whose return values can only be cached together.
// Example: Caching 1 of 3 journeys and reusing for other queries is impossible.

const CREATE_ATOMS_TABLE = `\
CREATE TABLE IF NOT EXISTS atoms_${V} (
	atoms_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	inputHash CHARACTER(32) NOT NULL,
	data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS atoms_${V}_created_idx
	ON atoms_${V} (created);
CREATE INDEX IF NOT EXISTS atoms_${V}_inputHash_idx
	ON atoms_${V} (inputHash);
CREATE INDEX IF NOT EXISTS atoms_${V}_data_idx
	ON atoms_${V} (data);`

const READ_ATOM = `\
SELECT data FROM atoms_${V}
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
LIMIT 1`

const WRITE_ATOM = `\
INSERT OR REPLACE INTO atoms_${V}
(atoms_id, created, method, inputHash, data)
VALUES ($id, $created, $method, $inputHash, $data)`

const createStore = (db) => {
	const init = (cb) => {
		debug('init')
		db.exec([
			CREATE_COLLECTION_QUERIES_TABLE,
			CREATE_COLLECTIONS_TABLE,
			CREATE_ATOMS_TABLE
		].join('\n'), cb)
	}

	const readCollection = (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax,
			createdMin, createdMax
		} = args
		const rowToVal = args.rowToVal || (row => JSON.parse(row.data))

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
				try {
					// todo: expose `.created`
					resolve(rows.map(rowToVal))
				} catch (err) {
					reject(err)
				}
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

	const readAtom = (method, inputHash, createdMin, createdMax, deserialize) => {
		debug('readAtom', {method, inputHash, createdMin, createdMax, deserialize})
		deserialize = deserialize || JSON.parse

		return new Promise((resolve, reject) => {
			db.get(READ_ATOM, {
				'$method': method,
				'$inputHash': inputHash,
				'$createdMin': Math.floor(createdMin / 1000),
				'$createdMax': Math.ceil(createdMax / 1000)
			}, (err, row) => {
				if (err) return reject(err)
				if (!row || !row.data) return resolve(null)
				try {
					resolve(deserialize(row.data))
				} catch (err) {
					reject(err)
				}
			})
		})
	}

	const writeAtom = (method, inputHash, created, val, serialize) => {
		debug('writeAtom', {method, inputHash, created, val, serialize})
		serialize = serialize || JSON.stringify

		return new Promise((resolve, reject) => {
			db.run(WRITE_ATOM, {
				'$id': randomBytes(10).toString('hex'),
				'$created': created / 1000 | 0,
				'$method': method,
				'$inputHash': inputHash,
				'$data': serialize(val)
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
		readAtom,
		writeAtom
	}
}

module.exports = createStore
