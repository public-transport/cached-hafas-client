import tape from 'tape'
import _tapePromise from 'tape-promise'
const {default: tapePromise} = _tapePromise
import pRetry from 'p-retry'

import {createSqliteStore} from '../stores/sqlite.js'
import {createRedisStore} from '../stores/redis.js'
import {createInMemoryStore} from '../stores/in-memory.js'
import {createCachedHafasClient as createCachedHafas} from '../index.js'

import {
	hafas,
	when,
	createSpy, delay,
	createSqliteDb, createRedisDb
} from './util.js'

// fake the Redis/SQLite helper API
const createInMemDb = async () => ({
	db: {},
	teardown: async () => {},
})

const second = 1000
const minute = 60 * second
const hour = 60 * minute

const wollinerStr = '900007105'
const husemannstr = '900110511'
const torfstr17 = {
	type: 'location',
	id: '770006698',
	address: '13353 Berlin-Wedding, Torfstr. 17',
	latitude: 52.541797,
	longitude: 13.350042
}

const journeysMock = async (from, to, opt = {}) => ({
	earlierRef: 'foo',
	laterRef: 'bar',
	journeys: [],
	realtimeDataFrom: null,
})

const rejects = async (t, fn) => {
	try {
		await fn()
	} catch (err) {
		t.ok(fn.name + ' rejected')
		return;
	}
	t.fail(fn.name + ' did not reject')
}

const test = tapePromise(tape)

const runTests = (storeName, createDb, createStore) => {
	const withMocksAndCache = async (hafas, mocks, ttl = hour, cachePeriods = {}) => {
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
				...cachePeriods,
			}
		})
		return {hafas: cachedMocked, store, teardown}
	}

	test(storeName + ' departures: same timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)
		const r2 = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)

		t.deepEqual({
			...r1,
			realtimeDataUpdatedAt: null,
		}, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' departures: shorter timespan -> reads from cache', async (t) => {
		const spy = createSpy(hafas.departures)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const {
			departures: allDeps,
		} = await h.departures(wollinerStr, {when, duration: 10})
		t.equal(spy.callCount, 1)

		const when2 = +new Date(+when + 3 * minute)
		const expectedDeps = allDeps.filter((dep) => {
			const w = +new Date(dep.when)
			return w >= when2 && w <= (3 * minute + when2)
		})

		const {
			departures: actualDeps,
		} = await h.departures(wollinerStr, {
			when: when2,
			duration: 3
		})
		t.equal(spy.callCount, 1)
		t.deepEqual(actualDeps.map(dep => dep.when), expectedDeps.map(dep => dep.when))
		await teardown()
		t.end()
	})

	test(storeName + ' departures: compares dep.when properly', async (t) => {
		const spy = createSpy(async () => ({
			departures: [
				{when: '2020-11-11T11:00+01:00'},
				{when: '2020-11-11T12:11+02:00'},
				{when: '2020-11-11T11:22+01:00'},
			],
		}))
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {departures: spy})

		const r1 = await h.departures(wollinerStr, {
			duration: 30, when: '2020-11-11T11:00+01:00',
		})
		t.equal(r1.departures.length, 3)

		// '2020-11-11T11:20+01:00' < '2020-11-11T12:11+02:00' // true
		// Date.parse('2020-11-11T11:20+01:00') < Date.parse('2020-11-11T12:11+02:00') // false
		const r2 = await h.departures(wollinerStr, {
			duration: 20, when: '2020-11-11T11:00+01:00',
		})
		t.equal(r2.departures.length, 2)

		await teardown()
		t.end()
	})

	test(storeName + ' departures: compares opt.when properly', async (t) => {
		const spy = createSpy(async () => ({
			departures: [
				{when: '2020-11-11T11:00+01:00'},
				{when: '2020-11-11T12:11+02:00'},
				{when: '2020-11-11T11:22+01:00'},
			],
		}))
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

		t.deepEqual({
			...r1,
			realtimeDataUpdatedAt: null,
		}, r2)
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

	test(storeName + ' journeys: caching works with default caching period', async (t) => {
		const journeysSpy = createSpy(journeysMock)
		const mockedHafas = Object.create(hafas)
		mockedHafas.journeys = journeysSpy

		const {db, teardown} = await createDb()
		const store = createStore(db)
		const h = createCachedHafas(mockedHafas, store)
		const opt = {departure: when}

		await h.journeys(wollinerStr, husemannstr, opt)
		t.equal(journeysSpy.callCount, 1)
		await h.journeys(wollinerStr, husemannstr, {...opt})
		t.equal(journeysSpy.callCount, 1) // caching worked!

		await teardown()
		t.end()
	})

	test(storeName + ' journeys: caching works with custom caching period fn', async (t) => {
		const journeysSpy = createSpy(journeysMock)
		const cachePeriodSpy = createSpy(() => 10 * second)
		const defaultTtl = 0
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {
			journeys: journeysSpy,
		}, defaultTtl, {
			journeys: cachePeriodSpy,
		})
		const opt = {departure: when}

		await h.journeys(wollinerStr, husemannstr, opt)
		t.equal(journeysSpy.callCount, 1)
		t.equal(cachePeriodSpy.callCount, 1)
		await h.journeys(wollinerStr, husemannstr, {...opt})
		t.equal(journeysSpy.callCount, 1) // caching worked!
		t.equal(cachePeriodSpy.callCount, 2) // caching period fn called again

		{
			const defaultTtl = 0
			const cachePeriod = () => null
			const {hafas: h, teardown} = await withMocksAndCache(hafas, {
				journeys: journeysSpy,
			}, defaultTtl, {
				journeys: cachePeriod,
			})

			await h.journeys(wollinerStr, husemannstr, {...opt})
			t.equal(journeysSpy.callCount, 2) // didn't use the cache!
		}

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

	const pTripId = hafas.journeys(wollinerStr, husemannstr, {
		departure: when,
		results: 1,
		stopovers: false
	})
	.then(({journeys}) => {
		const leg = journeys[0].legs.find(leg => leg.mode !== 'walking')
		return leg.tripId
	})

	test(storeName + ' trip: same arguments -> reads from cache', async (t) => {
		const spy = createSpy(hafas.trip)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {trip: spy})

		const id = await pTripId
		const opt = {when, stopovers: true}

		const r1 = await h.trip(id, opt)
		t.equal(spy.callCount, 1)
		const r2 = await h.trip(id, Object.assign({}, opt))
		t.equal(spy.callCount, 1)

		t.deepEqual(r1, r2)
		await teardown()
		t.end()
	})

	test(storeName + ' trip: different params -> fetches new', async (t) => {
		const spy = createSpy(hafas.trip)
		const {hafas: h, teardown} = await withMocksAndCache(hafas, {trip: spy})

		const id = await pTripId
		const opt = {when, stopovers: true}

		await h.trip(id, opt)
		t.equal(spy.callCount, 1)

		await h.trip(id + 'a', opt) // different `id`
		t.equal(spy.callCount, 2)
		await h.trip(id, {when, stopovers: false}) // different `opt`
		t.equal(spy.callCount, 3)
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

		const id = '900068201'
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

		const id = '900068201'
		const opt = {linesOfStops: true}

		await h.stop(id, opt)
		t.equal(spy.callCount, 1)

		await h.stop('900017101', opt) // different `id`
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

		await h.stop('900068201')
		t.equal(spy.callCount, 1)
		await h.stop('900068201')
		t.equal(spy.callCount, 1)

		await delay(2000)
		await h.stop('900068201')
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
		const {hafas: h, store, teardown} = await withMocksAndCache(hafas, {departures: spy})
		store.readCollection = createSpy(store.readCollection.bind(store))
		store.writeCollection = createSpy(store.writeCollection.bind(store))

		await h.departures(wollinerStr, {
			when,
		})
		t.equal(spy.callCount, 1)
		t.equal(store.readCollection.callCount, 0)
		t.equal(store.writeCollection.callCount, 0)

		await h.departures(wollinerStr, {
			when,
		})
		t.equal(spy.callCount, 2)
		t.equal(store.readCollection.callCount, 0)
		t.equal(store.writeCollection.callCount, 0)

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

test('silences cache failures', async (t) => {
	const dep = {when: '2020-11-11T11:11+01:00'}
	const journey = {id: 'abc'}
	const mockedHafas = {
		departures: async () => ({
			departures: [dep],
		}),
		journeys: async () => ({
			journeys: [journey],
		}),
	}
	const withStoreMocks = (storeMocks = {}) => {
		return createCachedHafas(mockedHafas, {
			init: async () => {},
			readCollection: async () => [],
			writeCollection: async () => {},
			readAtom: async () => null,
			writeAtom: async () => {},
			...storeMocks,
		})
	}

	await withStoreMocks({
		readCollection: () => {throw new Error('foo')},
	}).departures('123', {duration: 1})
	await withStoreMocks({
		readCollection: async () => {throw new Error('foo')},
	}).departures('123', {duration: 1})
	await rejects(t, async () => {
		await withStoreMocks({
			readCollection: async () => {throw new TypeError('foo')},
		}).departures('123', {duration: 1})
	})
	await rejects(t, async () => {
		await withStoreMocks({
			readCollection: async () => {throw new ReferenceError('foo')},
		}).departures('123', {duration: 1})
	})
	await rejects(t, async () => {
		await withStoreMocks({
			readCollection: async () => {throw new RangeError('foo')},
		}).departures('123', {duration: 1})
	})

	await withStoreMocks({
		writeCollection: () => {throw new Error('foo')},
	}).departures('123', {duration: 1})
	await withStoreMocks({
		writeCollection: async () => {throw new Error('foo')},
	}).departures('123', {duration: 1})
	await rejects(t, async () => {
		await withStoreMocks({
			writeCollection: async () => {throw new TypeError('foo')},
		}).departures('123', {duration: 1})
	})
	await rejects(t, async () => {
		await withStoreMocks({
			writeCollection: async () => {throw new ReferenceError('foo')},
		}).departures('123', {duration: 1})
	})
	await rejects(t, async () => {
		await withStoreMocks({
			writeCollection: async () => {throw new RangeError('foo')},
		}).departures('123', {duration: 1})
	})

	await withStoreMocks({
		readAtom: () => {throw new Error('foo')},
	}).journeys('123', '234')
	await withStoreMocks({
		readAtom: async () => {throw new Error('foo')},
	}).journeys('123', '234')
	await rejects(t, async () => {
		await withStoreMocks({
			readAtom: async () => {throw new TypeError('foo')},
		}).journeys('123', '234')
	})
	await rejects(t, async () => {
		await withStoreMocks({
			readAtom: async () => {throw new ReferenceError('foo')},
		}).journeys('123', '234')
	})
	await rejects(t, async () => {
		await withStoreMocks({
			readAtom: async () => {throw new RangeError('foo')},
		}).journeys('123', '234')
	})

	await withStoreMocks({
		writeAtom: () => {throw new Error('foo')},
	}).journeys('123', '234')
	await withStoreMocks({
		writeAtom: async () => {throw new Error('foo')},
	}).journeys('123', '234')
	await rejects(t, async () => {
		await withStoreMocks({
			writeAtom: async () => {throw new TypeError('foo')},
		}).journeys('123', '234')
	})
	await rejects(t, async () => {
		await withStoreMocks({
			writeAtom: async () => {throw new ReferenceError('foo')},
		}).journeys('123', '234')
	})
	await rejects(t, async () => {
		await withStoreMocks({
			writeAtom: async () => {throw new RangeError('foo')},
		}).journeys('123', '234')
	})
})

runTests('sqlite', createSqliteDb, createSqliteStore)
runTests('redis', createRedisDb, createRedisStore)
runTests('in-memory', createInMemDb, createInMemoryStore)
test.onFinish(() => {
	process.exit()
})

// todo: hit/miss events
