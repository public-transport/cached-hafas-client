'use strict'

const {createHash} = require('crypto')
const {stringify} = require('querystring')
const pick = require('lodash/pick')
const omit = require('lodash/omit')
const {EventEmitter} = require('events')

const MINUTE = 60 * 1000

const isObj = o => o && 'object' === typeof o && !Array.isArray(o)

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

const STORAGE_METHODS = ['init', 'readCollection', 'writeCollection', 'readAtom', 'writeAtom']

const createCachedHafas = async (hafas, storage, cachePeriod = MINUTE) => {
	if (!isObj(storage)) throw new Error('storage must be an object')
	for (const method of STORAGE_METHODS) {
		if ('function' !== typeof storage[method]) {
			throw new Error(`invalid storage: storage.${method} is not a function`)
		}
	}

	// arguments + time -> cache key
	const collectionWithCache = async (method, cacheKeyData, whenMin, duration, args, rowToVal, valToRow) => {
		const createdMax = Date.now()
		const createdMin = createdMax - cachePeriod
		const inputHash = hash(JSON.stringify(cacheKeyData))

		const values = await storage.readCollection({
			method, inputHash, whenMin, whenMax: whenMin + duration,
			createdMin, createdMax, cachePeriod,
			rowToVal
		})
		if (values.length > 0) {
			out.emit('hit', method, ...args, values.length)
			return values
		}
		out.emit('miss', method, ...args)

		const created = Date.now()
		const vals = await hafas[method](...args)
		await storage.writeCollection({
			method, inputHash, when: whenMin, duration,
			created, cachePeriod,
			rows: vals.map(valToRow)
		})
		return vals
	}

	// arguments -> cache key
	const atomWithCache = async (methodName, cacheKeyData, args) => {
		const createdMax = Date.now()
		const createdMin = createdMax - cachePeriod
		const inputHash = hash(JSON.stringify(cacheKeyData))

		const cached = await storage.readAtom({
			method: methodName, inputHash,
			createdMin, createdMax, cachePeriod,
		})
		if (cached) {
			out.emit('hit', methodName, ...args)
			return cached
		}
		out.emit('miss', methodName, ...args)

		const created = Date.now()
		const val = await hafas[methodName](...args)
		await storage.writeAtom({
			method: methodName, inputHash,
			created, cachePeriod,
			val,
		})
		return val
	}

	const depsOrArrs = (method) => {
		const rowToVal = row => JSON.parse(row.data)
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
			], whenMin, duration, [stopId, opt], rowToVal, valToRow)
		}
		return query
	}

	const departures = depsOrArrs('departures')
	const arrivals = depsOrArrs('arrivals')

	const journeys = (from, to, opt = {}) => {
		return atomWithCache('journeys', [
			formatLocation(from),
			formatLocation(to),
			opt
		], [from, to, opt])
	}

	const refreshJourney = (refreshToken, opt = {}) => {
		return atomWithCache('refreshJourney', [refreshToken, opt], [refreshToken, opt])
	}

	const trip = (id, lineName, opt = {}) => {
		return atomWithCache('trip', [
			id,
			lineName,
			omit(opt, ['when'])
		], [id, lineName, opt])
	}

	const locations = (query, opt = {}) => {
		return atomWithCache('locations', [query, opt], [query, opt])
	}

	const stop = (id, opt = {}) => {
		return atomWithCache('stop', [id, opt], [id, opt])
	}

	// todo: cache individual locations, use a spatial index for querying
	const nearby = (loc, opt = {}) => {
		return atomWithCache('nearby', [
			formatLocation(loc),
			opt
		], [loc, opt])
	}

	// todo: cache individual movements, use a spatial index for querying
	const radar = (bbox, opt = {}) => {
		return atomWithCache('radar', [bbox, opt], [bbox, opt])
	}

	const reachableFrom = (address, opt = {}) => {
		let cacheOpt = opt
		if ('when' in cacheOpt) {
			cacheOpt = Object.assign({}, opt)
			cacheOpt.when = Math.round(new Date(cacheOpt.when) / 1000)
		}

		return atomWithCache('reachableFrom', [
			address,
			cacheOpt
		], [address, opt])
	}

	// initialize storage
	await storage.init()

	const out = new EventEmitter()
	out.departures = departures
	out.arrivals = arrivals
	out.journeys = journeys
	if (hafas.refreshJourney) out.refreshJourney = refreshJourney
	if (hafas.trip) out.trip = trip
	out.locations = locations
	out.stop = stop
	out.nearby = nearby
	if (hafas.radar) out.radar = radar
	if (hafas.reachableFrom) out.reachableFrom = reachableFrom
	return out
}

module.exports = createCachedHafas
