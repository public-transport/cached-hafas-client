'use strict'

const {stringify} = require('querystring')
const pick = require('lodash/pick')
const hash = require('hash-string')
const base58 = require('base58').encode
const series = require('async/series')
const {EventEmitter} = require('events')
// const debug = require('debug')('cached-hafas-client')

const MINUTE = 60 // we store seconds, not milliseconds
const CACHE_PERIOD = MINUTE

const CREATE_ARRIVALS_TABLE = `\
CREATE TABLE IF NOT EXISTS arrivals (
	stopId VARCHAR(15),
	"when" INT,
	tStored INT,
	tripId VARCHAR(25),
	optHash VARCHAR(20),
	data TEXT
)`

const READ_ARRIVALS = `\
SELECT data FROM arrivals WHERE \
	stopId = $stopId
	AND "when" >= $whenMin
	AND "when" <= $whenMax
	AND tStored >= $tStoredMin
	AND tStored <= $tStoredMax
	AND optHash = $optHash
ORDER BY "when"`

const WRITE_ARRIVALS = `\
INSERT OR REPLACE INTO arrivals
(stopId, "when", tStored, tripId, optHash, data)
VALUES ($stopId, $when, $tStored, $tripId, $optHash, $data)`

const arrivalsOptHash = (opt) => {
	const filtered = pick(opt, [
		'direction', 'stationLines', 'remarks', 'includeRelatedStations', 'language'
	])
	return base58(hash(stringify(filtered)))
}

const createCachedHafas = (hafas, db) => {
	const readArrivals = (stopId, whenMin, whenMax, tStoredMin, tStoredMax, optHash) => {
		return new Promise((resolve, reject) => {
			db.all(READ_ARRIVALS, {
				'$stopId': stopId.replace(leadingZeros, ''),
				'$whenMin': whenMin, '$whenMax': whenMax,
				'$tStoredMin': tStoredMin, '$tStoredMax': tStoredMax,
				'$optHash': optHash
			}, (err, rows) => {
				if (err) return reject(err)
				resolve(rows.map(row => JSON.parse(row.data)))
			})
		})
	}

	const leadingZeros = /^0+/
	const writeArrivals = (arrivals, optHash, tStored) => {
		const cmd = db.prepare(WRITE_ARRIVALS)
		for (let arr of arrivals) {
			cmd.bind({
				'$stopId': arr.stop.id.replace(leadingZeros, ''),
				'$when': new Date(arr.when) / 1000 | 0,
				'$tStored': tStored,
				'$tripId': arr.tripId,
				'$optHash': optHash,
				'$data': JSON.stringify(arr)
			})
		}
		return new Promise((resolve, reject) => {
			cmd.run((err) => {
				if (err) reject(err)
				else resolve()
			})
		})
	}

	const arrivals = async (stopId, opt = {}) => {
		const t = Date.now() / 1000 | 0
		const whenMin = (opt.when ? +new Date(opt.when) : Date.now()) / 1000 | 0
		const optHash = arrivalsOptHash(opt)

		if (opt.duration) {
			const whenMax = whenMin + opt.duration * MINUTE
			console.error(1, 'readArrivals', stopId, whenMin, whenMax, t - CACHE_PERIOD, t, optHash)
			const values = await readArrivals(stopId, whenMin, whenMax, t - CACHE_PERIOD, t, optHash)
			if (values.length > 0) {
				out.emit('hit', stopId, opt, values.length)
				return values
			}
			out.emit('miss', stopId, opt)
		}

		const arrivals = await hafas.arrivals(stopId, opt)
		await writeArrivals(arrivals, optHash, t)
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
			cb => db.run(CREATE_ARRIVALS_TABLE, cb),
			cb => db.run(`\
	CREATE UNIQUE INDEX IF NOT EXISTS arrivals_stopId_idx ON arrivals (stopId)`, cb),
			cb => db.run(`\
	CREATE UNIQUE INDEX IF NOT EXISTS arrivals_when_idx ON arrivals ("when")`, cb),
			cb => db.run(`\
	CREATE UNIQUE INDEX IF NOT EXISTS arrivals_tStored_idx ON arrivals (tStored)`, cb),
			cb => db.run(`\
	CREATE UNIQUE INDEX IF NOT EXISTS arrivals_tripId_idx ON arrivals (tripId)`, cb),
			cb => db.run(`\
	CREATE UNIQUE INDEX IF NOT EXISTS arrivals_optHash_idx ON arrivals (optHash)`, cb)
		], done)
	})
}

module.exports = createCachedHafas
