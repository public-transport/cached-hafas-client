'use strict'

const DEBUG = process.env.NODE_DEBUG === 'cached-hafas-client'

const {DateTime} = require('luxon')
const sqlite3 = DEBUG ? require('sqlite3').verbose() : require('sqlite3')
const createHafas = require('db-hafas')
const tape = require('tape')
const tapePromise = require('tape-promise').default

const createCachedHafas = require('.')

const when = new Date(DateTime.fromMillis(Date.now(), {
	zone: 'Europe/Berlin', // todo: use db-hafas timezone
	locale: 'de-DE', // todo: use db-hafas locale
})
.startOf('week').plus({weeks: 1, hours: 10})
.toISO())
const minute = 60 * 1000

const wollinerStr = '0730799'
const husemannstr = '0732658'

const hafas = createHafas('hafas-client-cache test')
const withMocksAndCache = (hafas, mocks) => {
	const mocked = Object.assign(Object.create(hafas), mocks)
	const db = new sqlite3.Database(':memory:')
	if (DEBUG) db.on('profile', query => console.debug(query))
	const cachedMocked = createCachedHafas(mocked, db)
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

// todo
// todo: removes from cache
// todo: hit/miss events
