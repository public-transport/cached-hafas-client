'use strict'

const {stringify} = require('querystring')
const pick = require('lodash/pick')
const hash = require('hash-string')
const base58 = require('base58').encode
const {randomBytes} = require('crypto')
const {EventEmitter} = require('events')
const series = require('async/series')
// const debug = require('debug')('cached-hafas-client')

const MINUTE = 60 * 1000
const CACHE_PERIOD = MINUTE

const CREATE_ARRIVALS_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS arr_queries (
	arr_queries_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	stopId VARCHAR(15) NOT NULL,
	"when" INT NOT NULL,
	duration INT NOT NULL,
	optHash VARCHAR(20) NOT NULL
);
CREATE INDEX IF NOT EXISTS arr_queries_created_idx ON arr_queries (created);
CREATE INDEX IF NOT EXISTS arr_queries_stopId_idx ON arr_queries (stopId);
CREATE INDEX IF NOT EXISTS arr_queries_when_idx ON arr_queries ("when");
CREATE INDEX IF NOT EXISTS arr_queries_duration_idx ON arr_queries (duration);
CREATE INDEX IF NOT EXISTS arr_queries_optHash_idx ON arr_queries (optHash);`

const CREATE_ARRIVALS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS arr (
	arr_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	"when" INT,
	tripId VARCHAR(25) NOT NULL,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES arr_queries(arr_queries_id)
);
CREATE INDEX IF NOT EXISTS arr_query_id_idx ON arr (query_id);`

const READ_ARRIVALS = `\
SELECT arr.data FROM arr_queries
LEFT JOIN arr ON arr_queries.arr_queries_id = arr.query_id
WHERE
	-- only find equal queries
	stopId = $stopId
	AND optHash = $optHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
	-- find queries that cover the when -> (when + duration) period
	AND arr_queries."when" <= $whenMin
	AND (arr_queries."when" + duration) >= $whenMax
`

const WRITE_ARRIVALS_QUERY = `\
INSERT OR REPLACE INTO arr_queries
(arr_queries_id, created, stopId, "when", duration, optHash)
VALUES ($id, $created, $stopId, $when, $duration, $optHash)`

const WRITE_ARRIVAL = `\
INSERT INTO arr
(arr_id, query_id, "when", tripId, data)
VALUES ($id, $queryId, $when, $tripId, $data)`

const arrivalsOptHash = (opt) => {
	const filtered = pick(opt, [
		'direction', 'stationLines', 'remarks', 'includeRelatedStations', 'language'
	])
	return base58(hash(stringify(filtered)))
}

const createCachedHafas = (hafas, db) => {
	const leadingZeros = /^0+/

	const readArrivals = (stopId, whenMin, whenMax, createdMin, createdMax, optHash) => {
		return new Promise((resolve, reject) => {
			db.all(READ_ARRIVALS, {
				'$stopId': stopId.replace(leadingZeros, ''),
				'$optHash': optHash,
				'$createdMin': createdMin / 1000 | 0,
				'$createdMax': createdMax / 1000 | 0,
				'$whenMin': whenMin / 1000 | 0,
				'$whenMax': whenMax / 1000 | 0
			}, (err, rows) => {
				if (err) return reject(err)
				resolve(rows.map(row => JSON.parse(row.data)))
			})
		})
	}

	const writeArrivals = async (stopId, when, duration, optHash, created, arrivals) => {
		const queryId = randomBytes(10).toString('hex')
		await new Promise((resolve, reject) => {
			db.run(WRITE_ARRIVALS_QUERY, {
				'$id': queryId,
				'$created': created / 1000 | 0,
				'$stopId': stopId,
				'$when': when / 1000 | 0,
				'$duration': duration * MINUTE / 1000 | 0,
				'$optHash': optHash
			}, err => err ? reject(err) : resolve())
		})

		// todo: use `cmd = db.prepare; cmd.bind` for performance!
		// const cmd = db.prepare(WRITE_ARRIVALS)
		for (let arr of arrivals) {
			await new Promise((resolve, reject) => {
				db.run(WRITE_ARRIVAL, {
					'$id': randomBytes(10).toString('hex'),
					'$queryId': queryId,
					'$when': new Date(arr.when) / 1000 | 0,
					'$tripId': arr.tripId,
					'$data': JSON.stringify(arr)
				}, err => err ? reject(err) : resolve())
			})
		}
		// await new Promise((resolve, reject) => {
		// 	cmd.finalize(err => err ? reject(err) : resolve())
		// })
	}

	const arrivals = async (stopId, opt = {}) => {
		const t = Date.now()
		const when = opt.when ? +new Date(opt.when) : Date.now()
		if (!('duration' in opt)) throw new Error('missing opt.duration')
		const optHash = arrivalsOptHash(opt)

		if (opt.duration) {
			const whenMax = when + opt.duration * MINUTE
			const values = await readArrivals(stopId, when, whenMax, t - CACHE_PERIOD, t, optHash)
			if (values.length > 0) {
				out.emit('hit', stopId, opt, values.length)
				return values
			}
			out.emit('miss', stopId, opt)
		}

		const arrivals = await hafas.arrivals(stopId, opt)
		await writeArrivals(stopId, when, opt.duration, optHash, t, arrivals)
		return arrivals
	}

	// todo

	const out = new EventEmitter()
	out.arrivals = arrivals
	return out

	// todo: run initDb and call cb here
	// todo: delete old entries
}

createCachedHafas.initDb = (db, done) => {
	db.serialize(() => {
		series([
			cb => db.exec(CREATE_ARRIVALS_QUERIES_TABLE, cb),
			cb => db.exec(CREATE_ARRIVALS_TABLE, cb)
		], done)
	})
}

module.exports = createCachedHafas
