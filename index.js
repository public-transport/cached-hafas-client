'use strict'

const {createHash} = require('crypto')
const {stringify} = require('querystring')
const pick = require('lodash/pick')
const omit = require('lodash/omit')
const {EventEmitter} = require('events')

const SECOND = 1000
const MINUTE = 60 * SECOND
const HOUR = 60 * MINUTE

const CACHED = Symbol.for('cached-hafas-client:cached')
const TIME = Symbol.for('cached-hafas-client:time')

const isObj = o => o && 'object' === typeof o && !Array.isArray(o)

const hash = (str) => {
	return createHash('sha256').update(str, 'utf8').digest('hex').slice(0, 32)
}

const round1000 = x => Math.round(x / 1000) * 1000

const formatLocation = (loc) => {
	if (!loc) throw new Error('invalid location! pass a string or an object.')
	if ('string' === typeof loc) return loc
	if (loc.type === 'station' || loc.type === 'stop') return loc.id
	if (loc.type) return JSON.stringify(loc)
	throw new Error('invalid location!')
}

const STORAGE_METHODS = ['init', 'readCollection', 'writeCollection', 'readAtom', 'writeAtom']

const createCachedHafas = (hafas, storage, opt = {}) => {
	if (!isObj(storage)) {
		throw new TypeError('storage must be an object')
	}
	for (const method of STORAGE_METHODS) {
		if ('function' !== typeof storage[method]) {
			throw new TypeError(`invalid storage: storage.${method} must be a function`)
		}
	}
	const cachePeriods = {
		// results contain (or calculation depends on) prognosed delays
		departures: 30 * SECOND, arrivals: 30 * SECOND,
		journeys: 30 * SECOND, refreshJourney: MINUTE,
		trip: 30 * SECOND,
		reachableFrom: 30 * SECOND,
		// results contain prognosed positions
		radar: 10 * SECOND,
		// rather static data
		locations: HOUR,
		stop: HOUR,
		nearby: HOUR,
		...(opt.cachePeriods || {}),
	}

	// initialize storage
	const pStorageInit = storage.init()

	// arguments + time -> cache key
	const collectionWithCache = async (method, useCache, cacheKeyData, whenMin, duration, args, rowToVal, valToRow) => {
		const t0 = Date.now()
		const inputHash = hash(JSON.stringify(cacheKeyData))
		const cachePeriod = cachePeriods[method] || 10 * SECOND
		await pStorageInit

		if (useCache) {
			const createdMax = round1000(Date.now())
			const createdMin = createdMax - cachePeriod
			const values = await storage.readCollection({
				method, inputHash,
				whenMin, whenMax: whenMin + duration,
				createdMin, createdMax, cachePeriod,
				rowToVal
			})
			if (values.length > 0) {
				out.emit('hit', method, ...args, values.length)
				Object.defineProperty(values, CACHED, {value: true})
				Object.defineProperty(values, TIME, {value: Date.now() - t0})
				return values
			}
		}
		out.emit('miss', method, ...args)

		const created = round1000(Date.now())
		const vals = await hafas[method](...args)
		await storage.writeCollection({
			method, inputHash, when: whenMin, duration,
			created, cachePeriod,
			rows: vals.map(valToRow)
		})
		Object.defineProperty(vals, TIME, {value: Date.now() - t0})
		return vals
	}

	// arguments -> cache key
	const atomWithCache = async (methodName, useCache, cacheKeyData, args) => {
		const t0 = Date.now()
		const inputHash = hash(JSON.stringify(cacheKeyData))
		const cachePeriod = cachePeriods[methodName] || 10 * SECOND
		await pStorageInit

		if (useCache) {
			const createdMax = round1000(Date.now())
			const createdMin = createdMax - cachePeriod
			const cached = await storage.readAtom({
				method: methodName, inputHash,
				createdMin, createdMax, cachePeriod,
			})
			if (cached) {
				out.emit('hit', methodName, ...args)
				Object.defineProperty(cached, CACHED, {value: true})
				Object.defineProperty(cached, TIME, {value: Date.now() - t0})
				return cached
			}
		}
		out.emit('miss', methodName, ...args)

		const created = round1000(Date.now())
		const val = await hafas[methodName](...args)
		await storage.writeAtom({
			method: methodName, inputHash,
			created, cachePeriod,
			val,
		})
		Object.defineProperty(val, TIME, {value: Date.now() - t0})
		return val
	}

	const depsOrArrs = (method) => {
		const rowToVal = row => JSON.parse(row.data)
		const valToRow = (arrOrDep) => ({
			when: arrOrDep.when,
			data: JSON.stringify(arrOrDep)
		})

		const query = (stopId, opt = {}) => {
			let useCache = opt[CACHED] !== false

			const whenMin = round1000(opt.when ? +new Date(opt.when) : Date.now())
			if (!('duration' in opt)) useCache = false
			const duration = opt.duration * MINUTE

			// todo: handle `results` properly
			return collectionWithCache(method, useCache, [
				stopId,
				omit(opt, ['when', 'duration'])
			], whenMin, duration, [stopId, opt], rowToVal, valToRow)
		}
		return query
	}

	const departures = depsOrArrs('departures')
	const arrivals = depsOrArrs('arrivals')

	const journeys = (from, to, opt = {}) => {
		const useCache = opt[CACHED] !== false
		return atomWithCache('journeys', useCache, [
			formatLocation(from),
			formatLocation(to),
			opt
		], [from, to, opt])
	}

	const refreshJourney = (refreshToken, opt = {}) => {
		return atomWithCache(
			'refreshJourney',
			opt[CACHED] !== false,
			[refreshToken, opt],
			[refreshToken, opt]
		)
	}

	const trip = (id, lineName, opt = {}) => {
		const useCache = opt[CACHED] !== false
		return atomWithCache('trip', useCache, [
			id,
			lineName,
			omit(opt, ['when'])
		], [id, lineName, opt])
	}

	const locations = (query, opt = {}) => {
		return atomWithCache(
			'locations',
			opt[CACHED] !== false,
			[query, opt],
			[query, opt]
		)
	}

	const stop = (id, opt = {}) => {
		return atomWithCache(
			'stop',
			opt[CACHED] !== false,
			[id, opt],
			[id, opt]
		)
	}

	// todo: cache individual locations, use a spatial index for querying
	const nearby = (loc, opt = {}) => {
		const useCache = opt[CACHED] !== false
		return atomWithCache('nearby', useCache, [
			formatLocation(loc),
			opt
		], [loc, opt])
	}

	// todo: cache individual movements, use a spatial index for querying
	const radar = (bbox, opt = {}) => {
		return atomWithCache(
			'radar',
			opt[CACHED] !== false,
			[bbox, opt],
			[bbox, opt]
		)
	}

	const reachableFrom = (address, opt = {}) => {
		let cacheOpt = opt
		if ('when' in cacheOpt) {
			cacheOpt = Object.assign({}, opt)
			cacheOpt.when = Math.round(+new Date(cacheOpt.when) / 1000)
		}

		const useCache = opt[CACHED] !== false
		return atomWithCache('reachableFrom', useCache, [
			address,
			cacheOpt
		], [address, opt])
	}

	const tripsByName = (lineNameOrFahrtNr, opt = {}) => {
		let cacheOpt = opt
		if ('when' in cacheOpt) {
			cacheOpt = Object.assign({}, opt)
			cacheOpt.when = round1000(+new Date(cacheOpt.when))
		}

		const useCache = opt[CACHED] !== false
		return atomWithCache('tripsByName', useCache, [
			lineNameOrFahrtNr,
			cacheOpt
		], [lineNameOrFahrtNr, opt])
	}

	const out = new EventEmitter()
	out.CACHED = CACHED
	out.TIME = TIME
	out.profile = hafas.profile

	out.departures = departures
	out.arrivals = arrivals
	out.journeys = journeys
	if (hafas.refreshJourney) out.refreshJourney = refreshJourney
	if (hafas.trip) out.trip = trip
	if (hafas.tripsByName) out.tripsByName = tripsByName
	out.locations = locations
	out.stop = stop
	out.nearby = nearby
	if (hafas.radar) out.radar = radar
	if (hafas.reachableFrom) out.reachableFrom = reachableFrom
	return out
}

module.exports = createCachedHafas
