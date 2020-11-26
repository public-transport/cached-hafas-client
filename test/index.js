'use strict'

const tape = require('tape')
const tapePromise = require('tape-promise').default
const pRetry = require('p-retry')

const createSqliteStore = require('../stores/sqlite')
const createRedisStore = require('../stores/redis')
const createInMemStore = require('../stores/in-memory')
const createCachedHafas = require('..')

const {
	hafas,
	when,
	createSpy, delay,
	createSqliteDb, createRedisDb
} = require('./util')

// fake the Redis/SQLite helper API
const createInMemDb = async () => ({
	db: {},
	teardown: async () => {},
})

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

const runTests = (storeName, createDb, createStore) => {
	const withMocksAndCache = async (hafas, mocks, ttl = hour) => {
		const mocked = Object.assign(Object.create(hafas), mocks)
		const {db, teardown} = await createDb()
		const store = createStore(db)
		const cachedMocked = createCachedHafas(mocked, store, {
			cachePeriods: {
				departures: ttl, arrivals: ttl,
				journeys: ttl, refreshJourney: ttl, trip: ttl,
				radar: ttl,
				locations: ttl, stop: ttl, nearby: ttl,
				reachableFrom: ttl,
			}
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

	test(storeName + ' departures: compares dep.when properly', async (t) => {
		const spy = createSpy(async () => [
				{when: '2020-11-11T11:00+01:00'},
				{when: '2020-11-11T12:11+02:00'},
				{when: '2020-11-11T11:22+01:00'},
			])
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {
			duration: 30, when: '2020-11-11T11:00+01:00',
		})
		t.equal(r1.length, 3)

		// '2020-11-11T11:20+01:00' < '2020-11-11T12:11+02:00' // true
		// Date.parse('2020-11-11T11:20+01:00') < Date.parse('2020-11-11T12:11+02:00') // false
		const r2 = await h.departures(wollinerStr, {
			duration: 20, when: '2020-11-11T11:00+01:00',
		})
		t.equal(r2.length, 2)

		await teardown()
		t.end()
	})

	test(storeName + ' departures: compares opt.when properly', async (t) => {
		const spy = createSpy(async () => [
				{when: '2020-11-11T11:00+01:00'},
				{when: '2020-11-11T12:11+02:00'},
				{when: '2020-11-11T11:22+01:00'},
			])
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		await h.departures(wollinerStr, {
			duration: 30, when: '2020-11-11T11:00+01:00',
		})
		t.equal(spy.callCount, 1)

		// '2020-11-11T11:11+01:00' < '2020-11-11T11:11+02:00' // true
		// Date.parse('2020-11-11T11:11+01:00') < Date.parse('2020-11-11T11:11+02:00') // false
		await h.departures(wollinerStr, {
			duration: 30, when: '2020-11-11T10:00+00:00',
		})
		t.equal(spy.callCount, 1)
		await h.departures(wollinerStr, {
			duration: 30, when: '2020-11-11T11:00+02:00',
		})
		t.equal(spy.callCount, 2)

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
		const opt = {departure: when, linesOfStops: true}

		const r1 = await h.journeys(wollinerStr, husemannstr, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.journeys(wollinerStr, husemannstr, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual({
			// JSON.stringify drops `undefined`
			earlierRef: undefined,
			laterRef: undefined,
			...r2
		}, r1)
		await teardown()
		t.end()
	})

	test(storeName + ' journeys: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.journeys)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {journeys: spy})

		await h.journeys(wollinerStr, husemannstr, {departure: when, linesOfStops: true})
		t.equal(spy.callCount, 1)

		await h.journeys(wollinerStr, husemannstr, {
			departure: new Date(+when + 3 * minute),
			linesOfStops: true
		})
		t.equal(spy.callCount, 2)
		await h.journeys(wollinerStr, husemannstr, {departure: when, linesOfStops: false})
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
	.then(({journeys}) => journeys[0].refreshToken)
	pJourneyRefreshToken.catch((err) => {
		console.error(err)
		process.exit(1)
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
	.then(({journeys}) => {
		const leg = journeys[0].legs.find(leg => leg.mode !== 'walking')
		return {id: leg.tripId, lineName: leg.line && leg.line.name}
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

	test(storeName + ' locations: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.locations)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {locations: spy})

		const query = 'berlin'
		const opt = {linesOfStops: true}

		const r1 = await h.locations(query, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.locations(query, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' locations: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.locations)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {locations: spy})

		const query = 'berlin'
		const opt = {linesOfStops: true}

		await h.locations(query, opt)
		t.equal(spy.callCount, 1)

		await h.locations('muenchen', opt) // different `query`
		t.equal(spy.callCount, 2)
		await h.locations(query, {linesOfStops: true, fuzzy: false}) // different `opt`
		t.equal(spy.callCount, 3)
		await teardown()
		t.end()
	})

	test(storeName + ' stop: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.stop)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {stop: spy})

		const id = '900000068201'
		const opt = {linesOfStops: true}

		const r1 = await h.stop(id, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.stop(id, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' stop: different arguments -> fetches new', async (t) => {
		const spy = createSpy(hafas.stop)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {stop: spy})

		const id = '900000068201'
		const opt = {linesOfStops: true}

		await h.stop(id, opt)
		t.equal(spy.callCount, 1)

		await h.stop('900000017101', opt) // different `id`
		t.equal(spy.callCount, 2)
		await h.stop(id, {linesOfStops: true, language: 'en'}) // different `opt`
		t.equal(spy.callCount, 3)
		await teardown()
		t.end()
	})

	test(storeName + ' nearby: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.nearby)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {nearby: spy})

		const loc = {type: 'location', latitude: 52.5137344, longitude: 13.4744798}
		const opt = {distance: 400, linesOfStops: true}

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
		const opt = {distance: 400, linesOfStops: true}

		await h.nearby(loc, opt)
		t.equal(spy.callCount, 1)

		await h.nearby({type: 'location', latitude: 52.51, longitude: 13.47}, opt) // different `location`
		t.equal(spy.callCount, 2)
		await h.nearby(loc, {linesOfStops: true, language: 'de'}) // different `opt`
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
		const reachableFromWithRetry = (stop, opt) => {
			const run = () => hafas.reachableFrom(stop, opt)
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
		const spy = createSpy(hafas.stop)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {stop: spy}, ttl)

		await h.stop('900000068201')
		t.equal(spy.callCount, 1)
		await h.stop('900000068201')
		t.equal(spy.callCount, 1)

		await delay(2000)
		await h.stop('900000068201')
		t.equal(spy.callCount, 2)

		await teardown()
		t.end()
	})

	test(storeName + ' departures: bypassing the cache works', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {
			when, duration: 10, [h.CACHED]: false
		})
		t.equal(spy.callCount, 1)
		const r2 = await h.departures(wollinerStr, {
			when, duration: 10, [h.CACHED]: false
		})
		t.equal(spy.callCount, 2)
		await h.departures(wollinerStr, {
			when, duration: 10, [Symbol.for('cached-hafas-client:cached')]: false
		})
		t.equal(spy.callCount, 3)

		await teardown()
		t.end()
	})

	test(storeName + 'departures()/arrivals() without duration do not use the cache', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		await h.departures(wollinerStr, {
			when,
		})
		t.equal(spy.callCount, 1)

		await teardown()
		t.end()
	})

	test(storeName + ' rounds opt.when to seconds', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		await h.departures(wollinerStr, {
			duration: 3, when,
		})
		t.equal(spy.callCount, 1)

		await h.departures(wollinerStr, {
			duration: 3, when: +new Date(when) - 200, // 200ms earlier
		})
		t.equal(spy.callCount, 1)

		await teardown()
		t.end()
	})

	test(storeName + ' exposes CACHED boolean & TIME', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {
			when, duration: 2
		})
		t.ok(r1[h.CACHED] !== true)
		t.ok(Number.isInteger(r1[h.TIME]))

		const r2 = await h.departures(wollinerStr, {
			when, duration: 2
		})
		t.equal(r2[h.CACHED], true)
		t.ok(Number.isInteger(r2[h.TIME]))

		await teardown()
		t.end()
	})
}

runTests('sqlite', createSqliteDb, createSqliteStore)
runTests('redis', createRedisDb, createRedisStore)
runTests('in-memory', createInMemDb, createInMemStore)
test.onFinish(() => {
	process.exit()
})

// todo: hit/miss events
