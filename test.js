'use strict'

const DEBUG = process.env.NODE_DEBUG === 'cached-hafas-client'

const {DateTime} = require('luxon')
const sqlite3 = DEBUG ? require('sqlite3').verbose() : require('sqlite3')
const createHafas = require('vbb-hafas')
const tape = require('tape')
const tapePromise = require('tape-promise').default

const createSqliteStore = require('./stores/sqlite')
const createCachedHafas = require('.')

const when = new Date(DateTime.fromMillis(Date.now(), {
	zone: 'Europe/Berlin', // todo: use vbb-hafas timezone
	locale: 'de-DE', // todo: use vbb-hafas locale
})
.startOf('week').plus({weeks: 1, hours: 10})
.toISO())
const minute = 60 * 1000

const wollinerStr = '900000007105'
const husemannstr = '900000110511'
const torfstr17 = {
	type: 'location',
	address: '13353 Berlin-Wedding, Torfstr. 17',
	latitude: 52.541797,
	longitude: 13.350042
}

const hafas = createHafas('hafas-client-cache test')
const withMocksAndCache = (hafas, mocks) => {
	const mocked = Object.assign(Object.create(hafas), mocks)
	const db = new sqlite3.Database(':memory:')
	if (DEBUG) db.on('profile', query => console.debug(query))
	const store = createSqliteStore(db)
	const cachedMocked = createCachedHafas(mocked, store)
	return new Promise((resolve, reject) => {
		cachedMocked.init(err => err ? reject(err) : resolve(cachedMocked))
	})
}

const createSpy = (origFn) => {
	const spyFn = (...args) => {
		spyFn.callCount++
		return origFn.apply({}, args)
	}
	spyFn.callCount = 0
	return spyFn
}

const test = tapePromise(tape)

test('departures: same timespan -> reads from cache', async (t) => {
	const spy = createSpy(hafas.departures)
	const h = await withMocksAndCache(hafas, {departures: spy})

	await h.departures(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	await h.departures(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	t.end()
})

test('departures: shorter timespan -> reads from cache', async (t) => {
	const spy = createSpy(hafas.departures)
	const h = await withMocksAndCache(hafas, {departures: spy})

	await h.departures(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	await h.departures(wollinerStr, {
		when: new Date(+when + 3 * minute),
		duration: 3
	})
	t.equal(spy.callCount, 1)
	t.end()
})

test('departures: longer timespan -> fetches new', async (t) => {
	const spy = createSpy(hafas.departures)
	const h = await withMocksAndCache(hafas, {departures: spy})

	await h.departures(wollinerStr, {when, duration: 5})
	t.equal(spy.callCount, 1)
	await h.departures(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 2)
	t.end()
})

test('arrivals: same timespan -> reads from cache', async (t) => {
	const spy = createSpy(hafas.arrivals)
	const h = await withMocksAndCache(hafas, {arrivals: spy})

	await h.arrivals(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	await h.arrivals(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	t.end()
})

test('arrivals: shorter timespan -> reads from cache', async (t) => {
	const spy = createSpy(hafas.arrivals)
	const h = await withMocksAndCache(hafas, {arrivals: spy})

	await h.arrivals(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 1)
	await h.arrivals(wollinerStr, {
		when: new Date(+when + 3 * minute),
		duration: 3
	})
	t.equal(spy.callCount, 1)
	t.end()
})

test('arrivals: longer timespan -> fetches new', async (t) => {
	const spy = createSpy(hafas.arrivals)
	const h = await withMocksAndCache(hafas, {arrivals: spy})

	await h.arrivals(wollinerStr, {when, duration: 5})
	t.equal(spy.callCount, 1)
	await h.arrivals(wollinerStr, {when, duration: 10})
	t.equal(spy.callCount, 2)
	t.end()
})

test('journeys: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.journeys)
	const h = await withMocksAndCache(hafas, {journeys: spy})
	const opt = {departure: when, stationLines: true}

	await h.journeys(wollinerStr, husemannstr, opt)
	t.equal(spy.callCount, 1)
	await h.journeys(wollinerStr, husemannstr, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	// todo: results deep equal?
	t.end()
})

test('journeys: different arguments -> fetches new', async (t) => {
	const spy = createSpy(hafas.journeys)
	const h = await withMocksAndCache(hafas, {journeys: spy})

	await h.journeys(wollinerStr, husemannstr, {departure: when, stationLines: true})
	t.equal(spy.callCount, 1)

	await h.journeys(wollinerStr, husemannstr, {
		departure: new Date(+when + 3 * minute),
		stationLines: true
	})
	t.equal(spy.callCount, 2)
	await h.journeys(wollinerStr, husemannstr, {departure: when, stationLines: false})
	t.equal(spy.callCount, 3)
	await h.journeys(wollinerStr, husemannstr, {departure: when, stopovers: true})
	t.equal(spy.callCount, 4)

	t.end()
})

const pJourneyRefreshToken = hafas.journeys(wollinerStr, husemannstr, {
	departure: when,
	results: 1, stopovers: false, remarks: false
})
.then(([journey]) => journey.refreshToken)

test('refreshJourney: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.refreshJourney)
	const h = await withMocksAndCache(hafas, {refreshJourney: spy})

	const refreshToken = await pJourneyRefreshToken
	const opt = {stopovers: true}

	await h.refreshJourney(refreshToken, opt)
	t.equal(spy.callCount, 1)
	await h.refreshJourney(refreshToken, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	t.end()
})

test('refreshJourney: different arguments -> fetches new', async (t) => {
	const spy = createSpy(hafas.refreshJourney)
	const h = await withMocksAndCache(hafas, {refreshJourney: spy})

	const refreshToken = await pJourneyRefreshToken
	const opt = {stopovers: true}

	await h.refreshJourney(refreshToken, opt)
	t.equal(spy.callCount, 1)

	await h.refreshJourney(refreshToken + 'a', opt) // different `refreshToken`
	t.equal(spy.callCount, 2)
	await h.refreshJourney(refreshToken, {remarks: false}) // different `opt`
	t.equal(spy.callCount, 3)
	t.end()
})

const pTrip = hafas.journeys(wollinerStr, husemannstr, {
	departure: when,
	results: 1,
	stopovers: false
})
.then(([journey]) => {
	const leg = journey.legs.find(leg => leg.mode !== 'walking')
	return {id: leg.id, lineName: leg.line && leg.line.name}
})

test('trip: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.trip)
	const h = await withMocksAndCache(hafas, {trip: spy})

	const {id, lineName} = await pTrip
	const opt = {when, stopovers: true}

	await h.trip(id, lineName, opt)
	t.equal(spy.callCount, 1)
	await h.trip(id, lineName, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	t.end()
})

test('trip: different params -> fetches new', async (t) => {
	const spy = createSpy(hafas.trip)
	const h = await withMocksAndCache(hafas, {trip: spy})

	const {id, lineName} = await pTrip
	const opt = {when, stopovers: true}

	await h.trip(id, lineName, opt)
	t.equal(spy.callCount, 1)

	await h.trip(id + 'a', lineName, opt) // different `id`
	t.equal(spy.callCount, 2)
	await h.trip(id, lineName + 'a', opt) // different `lineName`
	t.equal(spy.callCount, 3)
	await h.trip(id, lineName, {when, stopovers: false}) // different `opt`
	t.equal(spy.callCount, 4)
	t.end()
})

test('station: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.station)
	const h = await withMocksAndCache(hafas, {station: spy})

	const id = '900000068201'
	const opt = {stationLines: true}

	await h.station(id, opt)
	t.equal(spy.callCount, 1)
	await h.station(id, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	t.end()
})

test('station: different arguments -> fetches new', async (t) => {
	const spy = createSpy(hafas.station)
	const h = await withMocksAndCache(hafas, {station: spy})

	const id = '900000068201'
	const opt = {stationLines: true}

	await h.station(id, opt)
	t.equal(spy.callCount, 1)

	await h.station('900000017101', opt) // different `id`
	t.equal(spy.callCount, 2)
	await h.station(id, {stationLines: true, language: 'en'}) // different `opt`
	t.equal(spy.callCount, 3)
	t.end()
})

test('nearby: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.nearby)
	const h = await withMocksAndCache(hafas, {nearby: spy})

	const loc = {type: 'location', latitude: 52.5137344, longitude: 13.4744798}
	const opt = {distance: 400, stationLines: true}

	await h.nearby(loc, opt)
	t.equal(spy.callCount, 1)
	await h.nearby(loc, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	t.end()
})

test('nearby: different arguments -> fetches new', async (t) => {
	const spy = createSpy(hafas.nearby)
	const h = await withMocksAndCache(hafas, {nearby: spy})

	const loc = {type: 'location', latitude: 52.5137344, longitude: 13.4744798}
	const opt = {distance: 400, stationLines: true}

	await h.nearby(loc, opt)
	t.equal(spy.callCount, 1)

	await h.nearby({type: 'location', latitude: 52.51, longitude: 13.47}, opt) // different `location`
	t.equal(spy.callCount, 2)
	await h.nearby(loc, {stationLines: true, language: 'de'}) // different `opt`
	t.equal(spy.callCount, 3)
	t.end()
})

test('radar: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.radar)
	const h = await withMocksAndCache(hafas, {radar: spy})

	const bbox = {
		north: 52.52411,
		west: 13.41002,
		south: 52.51942,
		east: 13.41709
	}
	const opt = {frames: 1, results: 100}

	await h.radar(bbox, opt)
	t.equal(spy.callCount, 1)
	await h.radar(bbox, Object.assign({}, opt))
	t.equal(spy.callCount, 1)
	t.end()
})

test('radar: different arguments -> fetches new', async (t) => {
	const spy = createSpy(hafas.radar)
	const h = await withMocksAndCache(hafas, {radar: spy})

	const bbox = {
		north: 52.52411,
		west: 13.41002,
		south: 52.51942,
		east: 13.41709
	}
	const opt = {frames: 1, results: 100}

	await h.radar(bbox, opt)
	t.equal(spy.callCount, 1)

	await h.radar(Object.assign({}, bbox, {south: 52}), opt) // different `bbox`
	t.equal(spy.callCount, 2)
	await h.radar(bbox, {frames: 1, results: 100, duration: 10}) // different `opt`
	t.equal(spy.callCount, 3)
	t.end()
})

test('reachableFrom: same arguments -> reads from cache', async (t) => {
	const spy = createSpy(hafas.reachableFrom)
	const h = await withMocksAndCache(hafas, {reachableFrom: spy})

	const opt = {maxTransfers: 2, maxDuration: 30, when: +when}
	const newWhen = +when + 100

	await h.reachableFrom(torfstr17, opt)
	t.equal(spy.callCount, 1)
	await h.reachableFrom(torfstr17, Object.assign({}, opt, {when: newWhen}))
	t.equal(spy.callCount, 1)
	t.end()
})

test('reachableFrom: different arguments -> fetches new', async (t) => {
	// todo: make this test reliable, e.g. by retrying with exponential pauses
	const spy = createSpy(hafas.reachableFrom)
	const h = await withMocksAndCache(hafas, {reachableFrom: spy})

	const newAddr = Object.assign({}, torfstr17, {address: torfstr17.address + 'foo'})
	const opt = {maxTransfers: 2, maxDuration: 30, when}
	const newWhen = +when + 5 * minute

	await h.reachableFrom(torfstr17, opt)
	t.equal(spy.callCount, 1)

	await h.reachableFrom(newAddr, opt)
	t.equal(spy.callCount, 2)
	await h.reachableFrom(torfstr17, Object.assign({}, opt, {when: newWhen}))
	t.equal(spy.callCount, 3)
	t.end()
})

// todo
// todo: removes from cache
// todo: hit/miss events
