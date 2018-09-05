# cached-hafas-client

**Pass in a [HAFAS client](https://github.com/public-transport/hafas-client), cache data from it.**

[![npm version](https://img.shields.io/npm/v/cached-hafas-client.svg)](https://www.npmjs.com/package/cached-hafas-client)
[![build status](https://api.travis-ci.org/derhuerst/cached-hafas-client.svg?branch=master)](https://travis-ci.org/derhuerst/cached-hafas-client)
![ISC-licensed](https://img.shields.io/github/license/derhuerst/cached-hafas-client.svg)
[![chat with me on Gitter](https://img.shields.io/badge/chat%20with%20me-on%20gitter-512e92.svg)](https://gitter.im/derhuerst)


## Installation

```shell
npm install cached-hafas-client
```


## Usage

Because `cached-hafas-client` caches HAFAS responses by "request signature", it is build on the assumption that HAFAS works deterministically. This is why, for example, you must pass `opt.duration` to [`departures()`](https://github.com/public-transport/hafas-client/blob/eddacd0091785155cdf734f1761d62dd9ab7ef06/docs/departures.md)/[`arrivals()`](https://github.com/public-transport/hafas-client/blob/eddacd0091785155cdf734f1761d62dd9ab7ef06/docs/arrivals.md).

```js
// create HAFAS client
const createHafas = require('vbb-hafas')
const hafas = createHafas('cached-hafas-client example')

// create in-memory SQLite store
const sqlite3 = require('sqlite3')
const createSqliteStore = require('cached-hafas-client/stores/sqlite')
const store = createSqliteStore(new sqlite3.Database(':memory:'))

// wrap HAFAS client with cache
const withCache = require('cached-hafas-client')
const cachedHafas = withCache(hafas, store)

cachedHafas.init((err) => { // initialize the DB
	if (err) return console.error(err)

	const wollinerStr = '900000007105'
	const husemannstr = '900000110511'
	const when = new Date(Date.now() + 60 * 60 * 1000)

	// will fetch from HAFAS
	cachedHafas.departures(wollinerStr, {duration: 10, when})
	.then(() => {
		// within the time frame of a recent departures() call -> will read from cache
		return cachedHafas.departures(wollinerStr, {
			duration: 3, when: new Date(+when + 3 * 60 * 1000)
		})
	})
	.then(console.log)
	.catch(console.error)
})
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


## Contributing

If you have a question or need support using `cached-hafas-client, please double-check your code and setup first. If you think you have found a bug or want to propose a feature, refer to [the issues page](https://github.com/derhuerst/cached-hafas-client/issues).
