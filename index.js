'use strict'

const {createHash} = require('crypto')
const base58 = require('base58').encode
const hashStr = require('hash-string')
const {stringify} = require('querystring')
const pick = require('lodash/pick')
const debug = require('debug')('cached-hafas-client')
const {EventEmitter} = require('events')

const createStorage = require('./lib/storage')

const MINUTE = 60 * 1000
const CACHE_PERIOD = MINUTE

const hash = (str) => {
	return createHash('sha256').update(str, 'utf8').digest('hex').slice(0, 32)
}

const optHash = (opt) => {
	const _opt = Object.create(null)
	for (const key in opt) _opt[key] = '' + opt[key]
	return base58(hashStr(stringify(_opt)))
}
const arrivalsOptHash = (opt) => {
	const filtered = pick(opt, [ // todo: use omit
		'direction', 'stationLines', 'remarks', 'includeRelatedStations', 'language'
	])
	return optHash(filtered)
}

const formatLocation = (loc) => {
	if (!loc) throw new Error('invalid location! pass a string or an object.')
	if ('string' === typeof loc) return loc
	if (loc.type === 'station' || loc.type === 'stop') return loc.id
	if (loc.type) return JSON.stringify(loc)
	throw new Error('invalid location!')
}

const createCachedHafas = (hafas, db) => {
	const storage = createStorage(db)

	const depsOrArrs = (method, queryUncached) => {
		const query = async (stopId, opt = {}) => {
			const t = Date.now()
			const when = opt.when ? +new Date(opt.when) : Date.now()
			if (!('duration' in opt)) throw new Error('missing opt.duration')
			const dur = opt.duration * MINUTE
			const optHash = arrivalsOptHash(opt)

			if (opt.duration) {
				const whenMax = when + opt.duration * MINUTE
				const values = await storage.readDepsOrArrs(method, stopId, when, whenMax, t - CACHE_PERIOD, t, optHash)
				if (values.length > 0) {
					out.emit('hit', method, stopId, opt, values.length)
					return values
				}
				out.emit('miss', method, stopId, opt)
			}

			const t2 = Date.now()
			const arrivals = await queryUncached(stopId, opt)
			await storage.writeDepsOrArrs(method, stopId, when, dur, optHash, t2, arrivals)
			return arrivals
		}
		return query
	}

	const departures = depsOrArrs('dep', hafas.departures)
	const arrivals = depsOrArrs('arr', hafas.arrivals)

	const journeys = async (from, to, opt = {}) => {
		const createdMax = Date.now()
		const createdMin = createdMax - CACHE_PERIOD
		const inputHash = hash(JSON.stringify([
			formatLocation(from),
			formatLocation(to),
			opt
		]))

		{
			const cached = await storage.readJourneys(inputHash, createdMin, createdMax)
			if (cached && cached.length > 0) {
				out.emit('hit', 'journeys', from, to, opt, cached.length)
				return cached
			}
			out.emit('miss', 'journeys', from, to, opt)
		}

		const created = Date.now()
		const journeys = await hafas.journeys(from, to, opt)
		await storage.writeJourneys(inputHash, created, journeys)
		return journeys
	}

	// todo

	const out = new EventEmitter()
	out.init = storage.init // todo: run init here
	out.departures = departures
	out.arrivals = arrivals
	out.journeys = journeys
	return out

	// todo: delete old entries
}

module.exports = createCachedHafas
