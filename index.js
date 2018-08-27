'use strict'

const {stringify} = require('querystring')
const pick = require('lodash/pick')
const hash = require('hash-string')
const base58 = require('base58').encode
const {randomBytes} = require('crypto')
const {EventEmitter} = require('events')
const series = require('async/series')
const debug = require('debug')('cached-hafas-client')

const MINUTE = 60 * 1000
const CACHE_PERIOD = MINUTE

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

const arrivalsOptHash = (opt) => {
	const filtered = pick(opt, [
		'direction', 'stationLines', 'remarks', 'includeRelatedStations', 'language'
	])
	return base58(hash(stringify(filtered)))
}

const createCachedHafas = (hafas, db) => {
	const leadingZeros = /^0+/

	const readDepsArrs = (type, stopId, whenMin, whenMax, createdMin, createdMax, optHash) => {
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

	const writeDepsArrs = async (type, stopId, when, duration, optHash, created, arrivals) => {
		const queryId = randomBytes(10).toString('hex')
		await new Promise((resolve, reject) => {
			const row = {
				'$id': queryId,
				'$type': type, // 'dep' or 'arr'
				'$created': created / 1000 | 0,
				'$stopId': stopId.replace(leadingZeros, ''),
				'$when': when / 1000 | 0,
				'$duration': duration * MINUTE / 1000 | 0,
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

	const depsOrArrs = (method) => {
		const query = async (stopId, opt = {}) => {
			const t = Date.now()
			const when = opt.when ? +new Date(opt.when) : Date.now()
			if (!('duration' in opt)) throw new Error('missing opt.duration')
			const optHash = arrivalsOptHash(opt)

			if (opt.duration) {
				const whenMax = when + opt.duration * MINUTE
				debug('readDepsArrs', stopId, when, whenMax, t - CACHE_PERIOD, t, optHash)
				const values = await readDepsArrs(method, stopId, when, whenMax, t - CACHE_PERIOD, t, optHash)
				if (values.length > 0) {
					out.emit('hit', method, stopId, opt, values.length)
					return values
				}
				out.emit('miss', method, stopId, opt)
			}

			const arrivals = await hafas.arrivals(stopId, opt)
			debug('writeDepsArrs', stopId, when, opt.duration, optHash, t, '...arrivals')
			await writeDepsArrs(method, stopId, when, opt.duration, optHash, t, arrivals)
			return arrivals
		}
		return query
	}

	const departures = depsOrArrs('dep')
	const arrivals = depsOrArrs('arr')

	// todo

	const out = new EventEmitter()
	out.departures = departures
	out.arrivals = arrivals
	return out

	// todo: run initDb and call cb here
	// todo: delete old entries
}

createCachedHafas.initDb = (db, done) => {
	db.serialize(() => {
		series([
			cb => db.exec(CREATE_DEPS_ARRS_QUERIES_TABLE, cb),
			cb => db.exec(CREATE_DEPS_ARRS_TABLE, cb)
		], done)
	})
}

module.exports = createCachedHafas
