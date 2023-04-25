import tape from 'tape'
import _tapePromise from 'tape-promise'
const {default: tapePromise} = _tapePromise
import {Registry} from 'prom-client'
import isDeepEqual from 'deep-equal'

import {createInMemoryStore} from '../stores/in-memory.js'
import {createCachedHafasClient as createCachedHafas} from '../index.js'
import {trackCachingMetrics} from '../with-metrics.js'

import {hafas, when} from './util.js'

const second = 1000

// prefix of all cached-hafas-client metrics
const PREF = 'cached_hafas_client_'

const test = tapePromise(tape)

test('trackCachingMetrics works', async (t) => {
	const mocked = Object.assign(Object.create(hafas), {
		departures: async (id, {when}) => ({
			departures: [{
				tripId: 'trip-1',
				when: new Date(5 * second + (+new Date(when))).toISOString(),
			}],
		}),
		locations: async (query) => [],
	})

	const ttl = 10 * second
	const store = createInMemoryStore()
	const cachedMocked = createCachedHafas(mocked, store, {
		cachePeriods: {
			departures: ttl, arrivals: ttl,
			journeys: ttl, refreshJourney: ttl, trip: ttl,
			radar: ttl,
			locations: ttl, stop: ttl, nearby: ttl,
			reachableFrom: ttl,
		},
	})

	const registry = new Registry()
	trackCachingMetrics(cachedMocked, {metricsRegistry: registry})
	const getMetrics = async (registry) => {
		return Object.fromEntries(
			(await registry.getMetricsAsJSON())
			.map(metric => [metric.name, metric])
		)
	}
	const getLabeledMetricVal = async (registry, metricName, labels) => {
		const metrics = await getMetrics(registry)
		const metric = metrics[metricName]
		const labeled = metric?.values.find(item => isDeepEqual(item.labels, labels))
		return labeled ? labeled.value : null
	}

	t.equal(
		await getLabeledMetricVal(registry, PREF + 'hits_total', {method: 'departures'}),
		null,
		PREF + 'hits_total[method=departures] is invalid',
	)
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'misses_total', {method: 'departures'}),
		null,
		PREF + 'misses_total[method=departures] is invalid',
	)

	await cachedMocked.departures('123', {when, duration: 180})
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'hits_total', {method: 'departures'}),
		null,
		PREF + 'hits_total[method=departures] is invalid',
	)
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'misses_total', {method: 'departures'}),
		1,
		PREF + 'misses_total[method=departures] is invalid',
	)

	await cachedMocked.departures('123', {when, duration: 180})
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'hits_total', {method: 'departures'}),
		1,
		PREF + 'hits_total[method=departures] is invalid',
	)
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'misses_total', {method: 'departures'}),
		1,
		PREF + 'misses_total[method=departures] is invalid',
	)

	await cachedMocked.locations('foOo?')
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'misses_total', {method: 'locations'}),
		1,
		PREF + 'misses_total[method=locations] is invalid',
	)
	// check if {hits,misses}_total[method=departures] is still unchanged
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'hits_total', {method: 'departures'}),
		1,
		PREF + 'hits_total[method=departures] is invalid',
	)
	t.equal(
		await getLabeledMetricVal(registry, PREF + 'misses_total', {method: 'departures'}),
		1,
		PREF + 'misses_total[method=departures] is invalid',
	)
})
