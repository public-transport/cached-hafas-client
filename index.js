'use strict'

const {createHash} = require('crypto')
const base58 = require('base58').encode
const hashStr = require('hash-string')
const {stringify} = require('querystring')
const pick = require('lodash/pick')
const omit = require('lodash/omit')
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

	const withCache = async (methodName, cacheKeyData, args) => {
		const createdMax = Date.now()
		const createdMin = createdMax - CACHE_PERIOD
		const inputHash = hash(JSON.stringify(cacheKeyData))

		const cached = await storage.readAtomic(methodName, inputHash, createdMin, createdMax)
		if (cached) {
			out.emit('hit', methodName, ...args, cached.length)
			return cached
		}
		out.emit('miss', methodName, ...args)

		const created = Date.now()
		const val = await hafas[methodName](...args)
		await storage.writeAtomic(methodName, inputHash, created, val)
		return val
	}

	const journeys = (from, to, opt = {}) => {
		return withCache('journeys', [
			formatLocation(from),
			formatLocation(to),
			opt
		], [from, to, opt])
	}

	const refreshJourney = (refreshToken, opt = {}) => {
		return withCache('refreshJourney', [refreshToken, opt], [refreshToken, opt])
	}

	const trip = (id, lineName, opt = {}) => {
		return withCache('trip', [
			id,
			lineName,
			omit(opt, ['when'])
		], [id, lineName, opt])
	}

	const station = (id, opt = {}) => {
		return withCache('station', [id, opt], [id, opt])
	}

	// todo: cache individual locations, use a spatial index for querying
	const nearby = (loc, opt = {}) => {
		return withCache('nearby', [
			formatLocation(loc),
			opt
		], [loc, opt])
	}

	// todo: cache individual movements, use a spatial index for querying
	const radar = (bbox, opt = {}) => {
		return withCache('radar', [bbox, opt], [bbox, opt])
	}

	// todo

	const out = new EventEmitter()
	out.init = storage.init // todo: run init here
	out.departures = departures
	out.arrivals = arrivals
	out.journeys = journeys
	if (hafas.refreshJourney) out.refreshJourney = refreshJourney
	if (hafas.trip) out.trip = trip
	out.station = station
	out.nearby = nearby
	if (hafas.radar) out.radar = radar
	return out

	// todo: delete old entries
}

module.exports = createCachedHafas
