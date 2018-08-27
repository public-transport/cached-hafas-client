'use strict'

const {stringify} = require('querystring')
const pick = require('lodash/pick')
const hash = require('hash-string')
const base58 = require('base58').encode
const debug = require('debug')('cached-hafas-client')
const {EventEmitter} = require('events')

const createStorage = require('./lib/storage')

const MINUTE = 60 * 1000
const CACHE_PERIOD = MINUTE

const arrivalsOptHash = (opt) => {
	const filtered = pick(opt, [
		'direction', 'stationLines', 'remarks', 'includeRelatedStations', 'language'
	])
	return base58(hash(stringify(filtered)))
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

			const arrivals = await queryUncached(stopId, opt)
			await storage.writeDepsOrArrs(method, stopId, when, dur, optHash, t, arrivals)
			return arrivals
		}
		return query
	}

	const departures = depsOrArrs('dep', hafas.departures)
	const arrivals = depsOrArrs('arr', hafas.arrivals)

	// todo

	const out = new EventEmitter()
	out.init = storage.init // todo: run init here
	out.departures = departures
	out.arrivals = arrivals
	return out

	// todo: delete old entries
}

module.exports = createCachedHafas
