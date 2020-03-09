'use strict'

const createHafas = require('vbb-hafas')
const {createClient: createRedis} = require('redis')
const withCache = require('.')
const createRedisStore = require('./stores/redis')

// const sqlite3 = require('sqlite3')
// const createSqliteStore = require('./stores/sqlite')
// const db = new sqlite3.Database(':memory:')
// const store = createSqliteStore(db)

const hafas = createHafas('cached-hafas-client example')
const db = createRedis()
const store = createRedisStore(db)
const cachedHafas = withCache(hafas, store)

cachedHafas.on('hit', (method, ...args) => console.info('cache hit!', method, ...args))
cachedHafas.on('miss', (method, ...args) => console.info('cache miss!', method, ...args))

cachedHafas.init((err) => {
	if (err) return onError(err)

	const wollinerStr = '900000007105'
	const husemannstr = '900000110511'
	const when = new Date(Date.now() + 60 * 60 * 1000)

	cachedHafas.departures(wollinerStr, {duration: 10, when})
	.then(() => cachedHafas.departures(wollinerStr, {duration: 3, when: new Date(+when + 3 * 60 * 1000)}))
	.then(deps => console.log(deps[0]))
	.catch(onError)

	cachedHafas.stop(wollinerStr)
	.then(() => cachedHafas.stop(wollinerStr))
	.then(console.log)
	.catch(onError)
})

const onError = (err) => {
	console.error(err)
	process.exit(1)
}
