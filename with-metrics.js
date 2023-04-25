import {
	register as globalMetricsRegistry,
	Counter, Summary, Gauge,
} from 'prom-client'

const trackHafasClientCachingMetrics = (cachedHafasClient, opt = {}) => {
	const {
		metricsRegistry,
	} = {
		metricsRegistry: globalMetricsRegistry,
		...opt,
	}
	if ('function' !== typeof cachedHafasClient.on) {
		throw new Error('cachedHafasClient does not seem to be compatible')
	}

	const hits = new Counter({
		name: 'cached_hafas_client_hits_total',
		help: 'cached-hafas-client: nr. of cache hits',
		registers: [metricsRegistry],
		labelNames: ['method'],
	})
	const trackHit = (method) => {
		hits.inc({method})
	}

	const misses = new Counter({
		name: 'cached_hafas_client_misses_total',
		help: 'cached-hafas-client: nr. of cache misses',
		registers: [metricsRegistry],
		labelNames: ['method'],
	})
	const trackMiss = (method) => {
		misses.inc({method})
	}

	cachedHafasClient.on('hit', trackHit)
	cachedHafasClient.on('miss', trackMiss)
	const stopTracking = () => {
		cachedHafasClient.removeListener('hit', trackHit)
		cachedHafasClient.removeListener('miss', trackMiss)
	}

	return {
		stopTracking,
	}
}

export {
	trackHafasClientCachingMetrics as trackCachingMetrics,
}
