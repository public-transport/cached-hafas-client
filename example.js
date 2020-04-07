'use strict'

const createHafas = require('vbb-hafas')
const {createClient: createRedis} = require('redis')
const withCache = require('.')
const createRedisStore = require('./stores/redis')

// using Redis
const db = createRedis()
const store = createRedisStore(db)

// using SQLite
// const sqlite3 = require('sqlite3')
// const createSqliteStore = require('./stores/sqlite')
// const db = new sqlite3.Database(':memory:')
// const store = createSqliteStore(db)

const wollinerStr = '900000007105'
const husemannstr = '900000110511'
const when = new Date(Date.now() + 60 * 60 * 1000)

;(async () => {

	const hafas = createHafas('cached-hafas-client example')
	const cachedHafas = withCache(hafas, store)

	cachedHafas.on('hit', (method, ...args) => console.info('cache hit!', method, ...args))
	cachedHafas.on('miss', (method, ...args) => console.info('cache miss!', method, ...args))

	await cachedHafas.departures(wollinerStr, {
		duration: 10, when
	})
	const deps = await cachedHafas.departures(wollinerStr, {
		duration: 3, when: new Date(+when + 3 * 60 * 1000)
	})
	console.log(deps[0])

	await cachedHafas.stop(wollinerStr)
	const stop = await cachedHafas.stop(wollinerStr)
	console.log(stop)

})()
.catch((err) => {
	console.error(err)
	process.exit(1)
})
