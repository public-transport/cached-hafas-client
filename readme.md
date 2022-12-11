# cached-hafas-client

**Pass in a [HAFAS client](https://github.com/public-transport/hafas-client/tree/6), cache data from it.**

[![npm version](https://img.shields.io/npm/v/cached-hafas-client.svg)](https://www.npmjs.com/package/cached-hafas-client)
[![build status](https://api.travis-ci.org/public-transport/cached-hafas-client.svg?branch=master)](https://travis-ci.org/public-transport/cached-hafas-client)
![ISC-licensed](https://img.shields.io/github/license/public-transport/cached-hafas-client.svg)
[![support me via GitHub Sponsors](https://img.shields.io/badge/support%20me-donate-fa7664.svg)](https://github.com/sponsors/derhuerst)
[![chat with me on Twitter](https://img.shields.io/badge/chat%20with%20me-on%20Twitter-1da1f2.svg)](https://twitter.com/derhuerst)

*Note:* This package is mainly **intended to prevent expensive and/or frequent API calls to HAFAS**. As a side effect, it *may* reduce local CPU load & latency, but that depends on the specific use case.

`cached-hafas-client`'s core logic is separated from data storage code; You can pick the store implementation that fits your use case best. Right now the following stores are implemented:

store name | built on top of | notes
-----------|-----------------|------
[`cached-hafas-client/stores/redis`](stores/redis.js) | [Redis](https://redis.io/) |
[`cached-hafas-client/stores/sqlite`](stores/sqlite.js) | [SQLite](https://www.sqlite.org/) | TTL not implemented yet
[`cached-hafas-client/stores/in-memory`](stores/in-memory.js) | in-memory (using [`quick-lru`](https://npmjs.com/package/quick-lru)) |


## Installation

```shell
npm install cached-hafas-client
```


## Usage

Let's set up a cached `hafas-client` instance.

```js
// create HAFAS client
const createHafas = require('vbb-hafas')
const hafas = createHafas('my-awesome-program')

// create a store backed by Redis
const Redis = require('ioredis')
const createRedisStore = require('cached-hafas-client/stores/redis')
const redis = new Redis()
const store = createRedisStore(redis)

// wrap HAFAS client with cache
const withCache = require('cached-hafas-client')
const cachedHafas = withCache(hafas, store)
```

Because `cached-hafas-client` caches HAFAS responses by "request signature", it is build on the assumption that, HAFAS works deterministically, aside from the ever-changing transit data underneath. Because there are no guarantees for this, use `cached-hafas-client` with a grain of salt.

This is why **you must send deterministic queries**; for example, you *must* pass `opt.duration` to [`departures()`](https://github.com/public-transport/hafas-client/blob/6/docs/departures.md)/[`arrivals()`](https://github.com/public-transport/hafas-client/blob/6/docs/arrivals.md), so that `cached-hafas-client` knows the time frame that the list of results returned by HAFAS is for.

```js
const wollinerStr = '900000007105'
const husemannstr = '900000110511'
const when = new Date(Date.now() + 60 * 60 * 1000)

// will fetch fresh data from HAFAS
await cachedHafas.departures(wollinerStr, {duration: 10, when})

// within the time frame of the departures() call above,
// so it will use the cached data
await cachedHafas.departures(wollinerStr, {
	duration: 3, when: new Date(+when + 3 * 60 * 1000)
})
```

*Note:* `cached-hafas-client` is only compatible with [`hafas-client@5`](https://github.com/public-transport/hafas-client/tree/6).

## with a custom cache TTL

By default, `cached-hafas-client` uses TTLs that try to strike a balance between up-to-date-ness and a cache hit ratio: The caching duration depends on how far in the future you query for.

You can pass custom cache TTLs per `hafas-client` method, either as static values or as a function returning the cache TTL based on the arguments.

```js
const SECOND = 1000
const MINUTE = 60 * SECOND

const cachePeriods = {
	// cache all cachedHafas.stop(…) calls for 10m
	stop: 10 * MINUTE,
	// cache cachedHafas.trip(tripId, opt) based on sqrt(opt.when - now)
	trip: (_, opt = {}) => {
		const diffSecs = (new Date(opt.when) - Date.now()) / SECOND
		if (Number.isNaN(diffSecs)) return 10 * SECOND // fallback
		return Math.round(Math.pow(diffSecs, 1/2) * SECOND)
	},
}
const cachedHafas = withCache(hafas, store, {cachePeriods})
```

## Counting cache hits & misses

```js
cachedHafas.on('hit', (hafasClientMethod, ...args) => {
	console.info('cache hit!', hafasClientMethod, ...args)
})
cachedHafas.on('miss', (hafasClientMethod, ...args) => {
	console.info('cache miss!', hafasClientMethod, ...args)
})
```

## Bypassing the cache

```js
const {CACHED} = require('cached-hafas-client')

// will always fresh data
await cachedHafas.departures(wollinerStr, {[CACHED]: false})
```


## API

```js
createCachedHafas(hafas, storage, opt = {})
```

`hafas` must be a [`hafas-client@6`](https://github.com/public-transport/hafas-client/tree/6)-compatible API client.

`opt` overrides this default configuration:

```js
{
	cachePeriods: {
		departures: 30_1000, arrivals: 30_1000, // 30s
		journeys: 30_1000, // 30s
		refreshJourney: 60_1000, // 1m
		trip: 30_1000, // 30s
		radar: 10_1000, // 10s
		locations: 3_600_1000, // 1h
		stop: 3_600_1000, // 1h
		nearby: 3_600_1000, // 1h
		reachableFrom: 30_1000,
	},
}
```


## Contributing

If you have a question or need support using `cached-hafas-client`, please double-check your code and setup first. If you think you have found a bug or want to propose a feature, use [the issues page](https://github.com/public-transport/cached-hafas-client/issues).
