'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')

const formatLocation = (loc) => {
	if (!loc) throw new Error('invalid location! pass a string or an object.')
	if ('string' === typeof loc) return loc
	if (loc.type === 'station' || loc.type === 'stop') return loc.id
	if (loc.type) return JSON.stringify(loc)
	throw new Error('invalid location!')
}

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

const CREATE_JOURNEY_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS journey_queries (
	journey_queries_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	"from" VARCHAR(20) NOT NULL,
	"to" VARCHAR(20) NOT NULL,
	optHash VARCHAR(20) NOT NULL
);
CREATE INDEX IF NOT EXISTS journey_queries_created_idx ON journey_queries (created);
CREATE INDEX IF NOT EXISTS journey_queries_from_idx ON journey_queries ("from");
CREATE INDEX IF NOT EXISTS journey_queries_to_idx ON journey_queries ("to");
CREATE INDEX IF NOT EXISTS journey_queries_optHash_idx ON journey_queries (optHash);`

const CREATE_JOURNEYS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS journeys (
	journeys_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES journey_queries(journey_queries_id)
);
CREATE INDEX IF NOT EXISTS journeys_query_id_idx ON journeys (query_id);`

const READ_JOURNEYS = `\
SELECT journeys.data FROM journey_queries
LEFT JOIN journeys ON journey_queries.journey_queries_id = journeys.query_id
WHERE
	-- only find equal queries
	"from" = $from
	AND "to" = $to
	AND optHash = $optHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax`

const WRITE_JOURNEYS_QUERY = `\
INSERT OR REPLACE INTO journey_queries
(journey_queries_id, created, "from", "to", optHash)
VALUES ($id, $created, $from, $to, $optHash)`

const WRITE_JOURNEY = `\
INSERT INTO journeys
(journeys_id, query_id, data)
VALUES ($id, $queryId, $data)`

const createStorage = (db) => {
	const init = (cb) => {
		debug('init')
		db.exec([
			CREATE_DEPS_ARRS_QUERIES_TABLE,
			CREATE_DEPS_ARRS_TABLE,
			CREATE_JOURNEY_QUERIES_TABLE,
			CREATE_JOURNEYS_TABLE
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

	const readJourneys = (from, to, optHash, createdMin, createdMax) => {
		debug('readJourneys', {from, to, optHash, createdMin, createdMax})

		// todo: refactor with readDepsOrArrs
		return new Promise((resolve, reject) => {
			const query = {
				'$from': formatLocation(from),
				'$to': formatLocation(to),
				'$optHash': optHash,
				'$createdMin': createdMin / 1000 | 0,
				'$createdMax': createdMax / 1000 | 0
			}
			debug('READ_JOURNEYS', query)
			db.all(READ_JOURNEYS, query, (err, rows) => {
				if (err) return reject(err)
				resolve(rows.map(row => JSON.parse(row.data)))
			})
		})
	}

	const writeJourneys = async (from, to, optHash, created, journeys) => {
		debug('writeJourneys', {from, to, optHash, created, journeys})
		const queryId = randomBytes(10).toString('hex')

		await new Promise((resolve, reject) => {
			const row = {
				'$id': queryId,
				'$from': formatLocation(from),
				'$to': formatLocation(to),
				'$optHash': optHash,
				'$created': created / 1000 | 0
			}
			debug('WRITE_JOURNEYS_QUERY', row)
			db.run(WRITE_JOURNEYS_QUERY, row, err => err ? reject(err) : resolve())
		})

		// todo: use `cmd = db.prepare; cmd.bind` for performance!
		// const cmd = db.prepare(WRITE_JOURNEY)
		for (let journey of journeys) {
			const row = {
				'$id': randomBytes(10).toString('hex'),
				'$queryId': queryId,
				'$data': JSON.stringify(journey)
			}
			debug('WRITE_JOURNEY', row)
			await new Promise((resolve, reject) => {
				db.run(WRITE_JOURNEY, row, err => err ? reject(err) : resolve())
			})
		}
		// await new Promise((resolve, reject) => {
		// 	cmd.finalize(err => err ? reject(err) : resolve())
		// })
	}

	return {
		init,
		readDepsOrArrs,
		writeDepsOrArrs,
		readJourneys,
		writeJourneys
	}
}

module.exports = createStorage
