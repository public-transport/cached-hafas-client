'use strict'

const {createHash} = require('crypto')
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

const formatLocation = (loc) => {
	if (!loc) throw new Error('invalid location! pass a string or an object.')
	if ('string' === typeof loc) return loc
	if (loc.type === 'station' || loc.type === 'stop') return loc.id
	if (loc.type) return JSON.stringify(loc)
	throw new Error('invalid location!')
}

const createCachedHafas = (hafas, db) => {
	const storage = createStorage(db)

	const collectionWithCache = async (method, cacheKeyData, whenMin, duration, args, valToRow) => {
		const createdMax = Date.now()
		const createdMin = createdMax - CACHE_PERIOD
		const inputHash = hash(JSON.stringify(cacheKeyData))

		const values = await storage.readCollection({
			method, inputHash,
			whenMin, whenMax: whenMin + duration,
			createdMin, createdMax
		})
		if (values.length > 0) {
			out.emit('hit', method, ...args, values.length)
			return values
		}
		out.emit('miss', method, ...args)

		const created = Date.now()
		const vals = await hafas[method](...args)
		await storage.writeCollection({
			method, inputHash,
			when: whenMin, duration,
			created, rows: vals.map(valToRow)
		})
		return vals
	}

	const depsOrArrs = (method) => {
		const valToRow = (arrOrDep) => ({
			when: arrOrDep.when,
			data: JSON.stringify(arrOrDep)
		})

		const query = (stopId, opt = {}) => {
			const whenMin = opt.when ? +new Date(opt.when) : Date.now()
			if (!('duration' in opt)) throw new Error('missing opt.duration')
			const duration = opt.duration * MINUTE

			return collectionWithCache(method, [
				stopId,
				omit(opt, ['when', 'duration'])
			], whenMin, duration, [stopId, opt], valToRow)
		}
		return query
	}

	const departures = depsOrArrs('departures')
	const arrivals = depsOrArrs('arrivals')

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

	const reachableFrom = (address, opt = {}) => {
		let cacheOpt = opt
		// todo: cache individually by `opt.when`
		if ('when' in cacheOpt) {
			cacheOpt = Object.assign({}, opt)
			cacheOpt.when = Math.round(new Date(cacheOpt.when) / 1000)
		}

		return withCache('reachableFrom', [
			address,
			cacheOpt
		], [address, opt])
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
	if (hafas.reachableFrom) out.reachableFrom = reachableFrom
	return out

	// todo: delete old entries
}

module.exports = createCachedHafas
