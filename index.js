import createDebug from 'debug'
import {strictEqual} from 'assert'
import {createHash} from 'crypto'
import {stringify} from 'querystring'
import pick from 'lodash/pick.js'
import omit from 'lodash/omit.js'
import {EventEmitter} from 'events'
import {NO_RESULTS} from './no-results.js'

const debug = createDebug('cached-hafas-client')

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

const silenceRejections = async (run) => {
	try {
		return await run()
	} catch (err) {
		debug('caching error', err)
		if (
			err instanceof RangeError ||
			err instanceof ReferenceError ||
			err instanceof TypeError
		) throw err
	}
	return NO_RESULTS
}

// todo: what about the past?
const dynamicCachePeriod = (multiplier, base, fallback, when) => {
	const secs = (new Date(when) - Date.now()) / 1000
	if (!Number.isNaN(secs) && secs > 0) {
		return Math.round(
			multiplier *
			Math.max(base, Math.pow(secs, 1/2))
			* SECOND
		)
	}
	return multiplier * fallback * SECOND
}
strictEqual(dynamicCachePeriod(1.5, 3, 10, new Date(Date.now() + 30 * SECOND).toISOString()), 8216, '30s from now')
strictEqual(dynamicCachePeriod(1.5, 3, 10, new Date(Date.now() + 30 * MINUTE).toISOString()), 63640, '30m from now')
strictEqual(dynamicCachePeriod(1.5, 3, 10, new Date(Date.now() + 30 * HOUR).toISOString()), 492950, '30h from now')

const createCachedHafasClient = (hafas, storage, opt = {}) => {
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
		// at least 10s, 20s fallback, 85s for query 30m from now
		departures: (_, opt = {}) => dynamicCachePeriod(2, 5, 10, opt.when),
		arrivals: (_, opt = {}) => dynamicCachePeriod(2, 5, 10, opt.when),
		trip: (_, opt = {}) => dynamicCachePeriod(2, 5, 10, opt.when),

		// results contain prognosed positions or highly dynamic results
		journeys: (_, __, opt = {}) => {
			const when = 'departure' in opt ? opt.departure : opt.arrival
			return dynamicCachePeriod(3, 4, 5, when)
		},
		refreshJourney: (_, opt = {}) => dynamicCachePeriod(3, 4, 5, opt.when),
		radar: (_, opt = {}) => dynamicCachePeriod(1, 5, 10, opt.when),

		// rather static data
		reachableFrom: (_, opt = {}) => dynamicCachePeriod(5, 12, 60, opt.when),
		locations: HOUR,
		stop: HOUR,
		nearby: HOUR,

		...(opt.cachePeriods || {}),
	}
	for (const [key, val] of Object.entries(cachePeriods)) {
		// todo [breaking]: always expect a function
		if ('function' === typeof val) continue
		if ('number' === typeof val) {
			cachePeriods[key] = () => val
			continue
		}
		throw new TypeError(`opt.cachePeriods.${key} must be a number or a function returning a number`)
	}

	// initialize storage
	const pStorageInit = storage.init()

	// arguments + time -> cache key
	const collectionWithCache = async (method, readFromCache, cacheKeyData, whenMin, duration, args, rowsToRes, resToRows) => {
		const t0 = Date.now()
		const inputHash = hash(JSON.stringify(cacheKeyData))
		const cachePeriod = method in cachePeriods
			? cachePeriods[method](...args)
			: 10 * SECOND
		if (cachePeriod === null) {
			debug('collectionWithCache', {
				method, readFromCache, whenMin, duration, args,
				inputHash, cachePeriod,
			}, 'not using cache because cachePeriods[method]() returned null')
			readFromCache = false
		} else if (!Number.isInteger(cachePeriod)) {
			throw new Error(`opt.cachePeriods.${method}() must return an integer or null`)
		}
		await pStorageInit

		if (readFromCache) {
			const createdMax = round1000(Date.now())
			const createdMin = createdMax - cachePeriod
			let values = await silenceRejections(storage.readCollection.bind(storage, {
				method, inputHash,
				whenMin, whenMax: whenMin + duration,
				createdMin, createdMax, cachePeriod,
			}))
			if (values !== NO_RESULTS) {
				out.emit('hit', method, ...args, values.length)

				const res = rowsToRes(values)
				Object.defineProperty(res, CACHED, {value: true})
				Object.defineProperty(res, TIME, {value: Date.now() - t0})
				return res
			}
		}
		out.emit('miss', method, ...args)

		const created = round1000(Date.now())
		const res = await hafas[method](...args)

		if (Number.isInteger(duration)) {
			await silenceRejections(storage.writeCollection.bind(storage, {
				method, inputHash, when: whenMin, duration,
				created, cachePeriod,
				rows: resToRows(res),
			}))
		}

		Object.defineProperty(res, TIME, {value: Date.now() - t0})
		return res
	}

	// arguments -> cache key
	const atomWithCache = async (methodName, readFromCache, cacheKeyData, args) => {
		const t0 = Date.now()
		const inputHash = hash(JSON.stringify(cacheKeyData))
		const cachePeriod = methodName in cachePeriods
			? cachePeriods[methodName](...args)
			: 10 * SECOND
		if (cachePeriod === null) {
			debug('atomWithCache', {
				methodName, readFromCache, args,
				inputHash, cachePeriod,
			}, 'not using cache because cachePeriods[method]() returned null')
			readFromCache = false
		} else if (!Number.isInteger(cachePeriod)) {
			throw new Error(`opt.cachePeriods.${methodName}() must return an integer or null`)
		}
		await pStorageInit

		if (readFromCache) {
			const createdMax = round1000(Date.now())
			const createdMin = createdMax - cachePeriod
			const cached = await silenceRejections(storage.readAtom.bind(storage, {
				method: methodName, inputHash,
				createdMin, createdMax, cachePeriod,
			}))
			if (cached !== NO_RESULTS) {
				out.emit('hit', methodName, ...args)
				if (cached) {
					Object.defineProperty(cached, CACHED, {value: true})
					Object.defineProperty(cached, TIME, {value: Date.now() - t0})
				}
				return cached
			}
		}
		out.emit('miss', methodName, ...args)

		const created = round1000(Date.now())
		const val = await hafas[methodName](...args)
		await silenceRejections(storage.writeAtom.bind(storage, {
			method: methodName, inputHash,
			created, cachePeriod,
			val,
		}))

		if (val) {
			Object.defineProperty(val, TIME, {value: Date.now() - t0})
		}
		return val
	}

	const depsOrArrs = (method) => {
		const rowsToRes = (rows) => {
			const arrsOrDeps = rows.map(row => JSON.parse(row.data))
			return {
				[method]: arrsOrDeps,
				// We cannot guess this value because each arrival/departure might have an
				// individual realtime data update timestamp.
				realtimeDataUpdatedAt: null,
			}
		}
		const resToRows = (res) => {
			return res[method].map((arrOrDep) => ({
				when: +new Date(arrOrDep.when),
				data: JSON.stringify(arrOrDep)
			}))
		}

		const query = (stopId, opt = {}) => {
			let useCache = opt[CACHED] !== false

			const whenMin = round1000(opt.when ? +new Date(opt.when) : Date.now())
			if (!('duration' in opt)) useCache = false
			const duration = opt.duration * MINUTE

			// todo: handle `results` properly
			return collectionWithCache(method, useCache, [
				stopId,
				omit(opt, ['when', 'duration'])
			], whenMin, duration, [stopId, opt], rowsToRes, resToRows)
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

	// todo: add journeysFromTrip() (DB profile only so far)

	const trip = (id, opt = {}) => {
		const useCache = opt[CACHED] !== false
		return atomWithCache('trip', useCache, [
			id,
			omit(opt, ['when'])
		], [id, opt])
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
		// todo: opt.when?
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
		let cacheOpt = Object.assign({}, opt)
		if ('when' in cacheOpt) {
			cacheOpt.when = round1000(+new Date(cacheOpt.when))
		}
		if ('fromWhen' in cacheOpt) {
			cacheOpt.fromWhen = round1000(+new Date(cacheOpt.fromWhen))
		}
		if ('fromWhen' in cacheOpt) {
			cacheOpt.untilWhen = round1000(+new Date(cacheOpt.untilWhen))
		}

		const useCache = opt[CACHED] !== false
		return atomWithCache('tripsByName', useCache, [
			lineNameOrFahrtNr,
			cacheOpt
		], [lineNameOrFahrtNr, opt])
	}

	const remarks = (opt = {}) => {
		return atomWithCache(
			'remarks',
			opt[CACHED] !== false,
			[opt],
			[opt]
		)
	}

	const lines = (query, opt = {}) => {
		return atomWithCache(
			'lines',
			opt[CACHED] !== false,
			[query, opt],
			[query, opt]
		)
	}

	const serverInfo = (opt = {}) => {
		return atomWithCache(
			'serverInfo',
			opt[CACHED] !== false,
			[opt],
			[opt]
		)
	}

	const out = new EventEmitter()
	Object.defineProperty(out, 'CACHED', {value: CACHED})
	Object.defineProperty(out, 'TIME', {value: TIME})
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
	if (hafas.remarks) out.remarks = serverInfo
	if (hafas.lines) out.lines = serverInfo
	if (hafas.serverInfo) out.serverInfo = serverInfo
	return out
}

export {
	CACHED,
	TIME,
	createCachedHafasClient,
}
