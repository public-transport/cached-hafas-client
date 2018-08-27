'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')

const CREATE_DEPS_ARRS_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS arr_dep_queries (
	arr_dep_queries_id CHARACTER(20) PRIMARY KEY,
	type CHARACTER(3), -- 'dep' or 'arr'
	created INT NOT NULL,
	stopId VARCHAR(15) NOT NULL,
	"when" INT NOT NULL,
	duration INT NOT NULL,
	optHash VARCHAR(20) NOT NULL
);
CREATE INDEX IF NOT EXISTS arr_dep_queries_created_idx ON arr_dep_queries (created);
CREATE INDEX IF NOT EXISTS arr_dep_queries_stopId_idx ON arr_dep_queries (stopId);
CREATE INDEX IF NOT EXISTS arr_dep_queries_when_idx ON arr_dep_queries ("when");
CREATE INDEX IF NOT EXISTS arr_dep_queries_duration_idx ON arr_dep_queries (duration);
CREATE INDEX IF NOT EXISTS arr_dep_queries_optHash_idx ON arr_dep_queries (optHash);`

const CREATE_DEPS_ARRS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS arr_dep (
	arr_dep_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	"when" INT,
	tripId VARCHAR(25) NOT NULL,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES arr_dep_queries(arr_dep_queries_id)
);
CREATE INDEX IF NOT EXISTS arr_query_id_idx ON arr_dep (query_id);`

const READ_DEPS_ARRS = `\
SELECT arr_dep.data FROM arr_dep_queries
LEFT JOIN arr_dep ON arr_dep_queries.arr_dep_queries_id = arr_dep.query_id
WHERE
	-- only find equal queries
	type = $type
	AND stopId = $stopId
	AND optHash = $optHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
	-- find queries that cover the when -> (when + duration) period
	AND arr_dep_queries."when" <= $whenMin
	AND (arr_dep_queries."when" + duration) >= $whenMax
`

const WRITE_DEPS_ARRS_QUERY = `\
INSERT OR REPLACE INTO arr_dep_queries
(arr_dep_queries_id, type, created, stopId, "when", duration, optHash)
VALUES ($id, $type, $created, $stopId, $when, $duration, $optHash)`

const WRITE_DEP_ARR = `\
INSERT INTO arr_dep
(arr_dep_id, query_id, "when", tripId, data)
VALUES ($id, $queryId, $when, $tripId, $data)`

const createStorage = (db) => {
	const init = (cb) => {
		debug('init')
		db.exec([
			CREATE_DEPS_ARRS_QUERIES_TABLE,
			CREATE_DEPS_ARRS_TABLE
		].join('\n'), cb)
	}

	const leadingZeros = /^0+/ // todo: move to index.js

	const readDepsOrArrs = (type, stopId, whenMin, whenMax, createdMin, createdMax, optHash) => {
		debug('readDepsOrArrs', {type, stopId, whenMin, whenMax, createdMin, createdMax, optHash})

		return new Promise((resolve, reject) => {
			const query = {
				'$type': type, // 'dep' or 'arr'
				'$stopId': stopId.replace(leadingZeros, ''),
				'$optHash': optHash,
				'$createdMin': createdMin / 1000 | 0,
				'$createdMax': createdMax / 1000 | 0,
				'$whenMin': whenMin / 1000 | 0,
				'$whenMax': whenMax / 1000 | 0
			}
			debug('READ_DEPS_ARRS', query)
			db.all(READ_DEPS_ARRS, query, (err, rows) => {
				if (err) return reject(err)
				resolve(rows.map(row => JSON.parse(row.data)))
			})
		})
	}

	const writeDepsOrArrs = async (type, stopId, when, duration, optHash, created, arrivals) => {
		debug('writeDepsOrArrs', {type, stopId, when, duration, optHash, created, arrivals})
		const queryId = randomBytes(10).toString('hex')

		await new Promise((resolve, reject) => {
			const row = {
				'$id': queryId,
				'$type': type, // 'dep' or 'arr'
				'$created': created / 1000 | 0,
				'$stopId': stopId.replace(leadingZeros, ''),
				'$when': when / 1000 | 0,
				'$duration': duration / 1000 | 0,
				'$optHash': optHash
			}
			debug('WRITE_DEPS_ARRS_QUERY', row)
			db.run(WRITE_DEPS_ARRS_QUERY, row, err => err ? reject(err) : resolve())
		})

		// todo: use `cmd = db.prepare; cmd.bind` for performance!
		// const cmd = db.prepare(WRITE_DEP_ARR)
		for (let arr of arrivals) {
			const row = {
				'$id': randomBytes(10).toString('hex'),
				'$queryId': queryId,
				'$when': new Date(arr.when) / 1000 | 0,
				'$tripId': arr.tripId,
				'$data': JSON.stringify(arr)
			}
			debug('WRITE_DEP_ARR', row)
			await new Promise((resolve, reject) => {
				db.run(WRITE_DEP_ARR, row, err => err ? reject(err) : resolve())
			})
		}
		// await new Promise((resolve, reject) => {
		// 	cmd.finalize(err => err ? reject(err) : resolve())
		// })
	}

	return {
		init,
		readDepsOrArrs,
		writeDepsOrArrs
	}
}

module.exports = createStorage
