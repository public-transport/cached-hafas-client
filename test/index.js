'use strict'

const createHafas = require('vbb-hafas')
const tape = require('tape')
const tapePromise = require('tape-promise').default
const pRetry = require('p-retry')

const createSqliteStore = require('../stores/sqlite')
const createRedisStore = require('../stores/redis')
const createCachedHafas = require('..')

const {
	when,
	createSpy, delay,
	createSqliteDb, createRedisDb
} = require('./util')

const minute = 60 * 1000
const hour = 60 * minute

const wollinerStr = '900000007105'
const husemannstr = '900000110511'
const torfstr17 = {
	type: 'location',
	address: '13353 Berlin-Wedding, Torfstr. 17',
	latitude: 52.541797,
	longitude: 13.350042
}

const test = tapePromise(tape)

const hafas = createHafas('cached-hafas-client test')

const runTests = (storeName, createDb, createStore) => {
	const withMocksAndCache = async (hafas, mocks, ttl = hour) => {
		const mocked = Object.assign(Object.create(hafas), mocks)
		const {db, teardown} = await createDb()
		const store = createStore(db)
		const cachedMocked = createCachedHafas(mocked, store, ttl)
		await new Promise((resolve, reject) => {
			cachedMocked.init(err => err ? reject(err) : resolve())
		})
		return {hafas: cachedMocked, teardown}
	}

	test(storeName + ' departures: same timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)
		const r2 = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' departures: shorter timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const allDeps = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)

		const when2 = +new Date(+when + 3 * minute)
		const expectedDeps = allDeps.filter((dep) => {
			const w = +new Date(dep.when)
			return w >= when2 && w <= (3 * minute + when2)
		})

		const actualDeps = await h.departures(wollinerStr, {
			when: when2,
			duration: 3
		})
		t.equal(spy.callCount, 1)
		t.deepEqual(actualDeps.map(dep => dep.when), expectedDeps.map(dep => dep.when))
		await teardown()
		t.end()
	})

	test(storeName + ' departures: longer timespan -> fetches new', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		await h.departures(wollinerStr, {when, duration: 5})
		t.equal(spy.callCount, 1)
		await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 2)
		await teardown()
		t.end()
	})

	test(storeName + ' arrivals: same timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.arrivals)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {arrivals: spy})

		const r1 = await h.arrivals(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)
		const r2 = await h.arrivals(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' arrivals: shorter timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.arrivals)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {arrivals: spy})

		await h.arrivals(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)
		await h.arrivals(wollinerStr, {
			when: new Date(+when + 3 * minute),
			duration: 3
		})
		t.equal(spy.callCount, 1)
		await teardown()
		t.end()
	})

	test(storeName + ' arrivals: longer timespan -> fetches new', async (t) => {
		const spy = createSpy(hafas.arrivals)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {arrivals: spy})

		await h.arrivals(wollinerStr, {when, duration: 5})
		t.equal(spy.callCount, 1)
		await h.arrivals(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 2)
		await teardown()
		t.end()
	})

	test(storeName + ' journeys: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.journeys)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {journeys: spy})
		const opt = {departure: when, stationLines: true}

		const r1 = await h.journeys(wollinerStr, husemannstr, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.journeys(wollinerStr, husemannstr, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' journeys: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.journeys)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {journeys: spy})

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

await teardown()
		t.end()
	})

	const pJourneyRefreshToken = hafas.journeys(wollinerStr, husemannstr, {
		departure: when,
		results: 1, stopovers: false, remarks: false
	})
	.then(([journey]) => journey.refreshToken)
	pJourneyRefreshToken.catch((err) => {
		console.error(err)
		process.exitCode = 1
	})

	test(storeName + ' refreshJourney: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.refreshJourney)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {refreshJourney: spy})

		const refreshToken = await pJourneyRefreshToken
		const opt = {stopovers: true}

		const r1 = await h.refreshJourney(refreshToken, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.refreshJourney(refreshToken, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' refreshJourney: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.refreshJourney)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {refreshJourney: spy})

		const refreshToken = await pJourneyRefreshToken
		const opt = {stopovers: true}

		await h.refreshJourney(refreshToken, opt)
		t.equal(spy.callCount, 1)

		await h.refreshJourney(refreshToken + 'a', opt) // different `refreshToken`
		t.equal(spy.callCount, 2)
		await h.refreshJourney(refreshToken, {remarks: false}) // different `opt`
		t.equal(spy.callCount, 3)
		await teardown()
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

	test(storeName + ' trip: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.trip)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {trip: spy})

		const {id, lineName} = await pTrip
		const opt = {when, stopovers: true}

		const r1 = await h.trip(id, lineName, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.trip(id, lineName, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' trip: different params -> fetches new', async (t) => {
		const spy = createSpy(hafas.trip)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {trip: spy})

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
		await teardown()
		t.end()
	})

	test(storeName + ' station: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.station)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {station: spy})

		const id = '900000068201'
		const opt = {stationLines: true}

		const r1 = await h.station(id, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.station(id, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' station: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.station)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {station: spy})

		const id = '900000068201'
		const opt = {stationLines: true}

		await h.station(id, opt)
		t.equal(spy.callCount, 1)

		await h.station('900000017101', opt) // different `id`
		t.equal(spy.callCount, 2)
		await h.station(id, {stationLines: true, language: 'en'}) // different `opt`
		t.equal(spy.callCount, 3)
		await teardown()
		t.end()
	})

	test(storeName + ' nearby: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.nearby)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {nearby: spy})

		const loc = {type: 'location', latitude: 52.5137344, longitude: 13.4744798}
		const opt = {distance: 400, stationLines: true}

		const r1 = await h.nearby(loc, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.nearby(loc, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' nearby: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.nearby)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {nearby: spy})

		const loc = {type: 'location', latitude: 52.5137344, longitude: 13.4744798}
		const opt = {distance: 400, stationLines: true}

		await h.nearby(loc, opt)
		t.equal(spy.callCount, 1)

		await h.nearby({type: 'location', latitude: 52.51, longitude: 13.47}, opt) // different `location`
		t.equal(spy.callCount, 2)
		await h.nearby(loc, {stationLines: true, language: 'de'}) // different `opt`
		t.equal(spy.callCount, 3)
		await teardown()
		t.end()
	})

	test(storeName + ' radar: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.radar)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {radar: spy})

		const bbox = {
			north: 52.52411,
			west: 13.41002,
			south: 52.51942,
			east: 13.41709
		}
		const opt = {frames: 1, results: 100}

		const r1 = await h.radar(bbox, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.radar(bbox, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' radar: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.radar)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {radar: spy})

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
		await teardown()
		t.end()
	})

	test(storeName + ' reachableFrom: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.reachableFrom)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {reachableFrom: spy})

		const opt = {maxTransfers: 2, maxDuration: 30, when: +when}
		const newWhen = +when + 100

		const r1 = await h.reachableFrom(torfstr17, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.reachableFrom(torfstr17, Object.assign({}, opt, {when: newWhen}))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' reachableFrom: different arguments -> fetches new', async (t) => {
		// todo: make this test reliable, e.g. by retrying with exponential pauses
		const reachableFromWithRetry = (station, opt) => {
			const run = () => hafas.reachableFrom(station, opt)
			return pRetry(run, {retries: 5, minTimeout: 2000, factor: 2})
		}
		const spy = createSpy(reachableFromWithRetry)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {reachableFrom: spy})

		const newAddr = Object.assign({}, torfstr17, {address: torfstr17.address + 'foo'})
		const opt = {maxTransfers: 2, maxDuration: 30, when}
		const newWhen = +when + 5 * minute

		await h.reachableFrom(torfstr17, opt)
		t.equal(spy.callCount, 1)

		await h.reachableFrom(newAddr, opt)
		t.equal(spy.callCount, 2)
		await h.reachableFrom(torfstr17, Object.assign({}, opt, {when: newWhen}))
		t.equal(spy.callCount, 3)
		await teardown()
		t.end()
	})

	test(storeName + ' should not give items older than cachePeriod', async (t) => {
		const ttl = 1000 // 1 second
		const spy = createSpy(hafas.station)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {station: spy}, ttl)

		await h.station('900000068201')
		t.equal(spy.callCount, 1)
		await h.station('900000068201')
		t.equal(spy.callCount, 1)

		await delay(2000)
		await h.station('900000068201')
		t.equal(spy.callCount, 2)

		await teardown()
		t.end()
	})
}

runTests('sqlite', createSqliteDb, createSqliteStore)
runTests('redis', createRedisDb, createRedisStore)

// todo: hit/miss events