import createHafas from 'vbb-hafas'
import {createCachedHafasClient as withCache} from './index.js'

// using Redis
import Redis from 'ioredis'
import {createRedisStore} from './stores/redis.js'
const db = new Redis()
const store = createRedisStore(db)

const MINUTE = 60 * 1000

// using SQLite
// const sqlite3 = require('sqlite3')
// const createSqliteStore = require('./stores/sqlite')
// const db = new sqlite3.Database(':memory:')
// const store = createSqliteStore(db)

const wollinerStr = '900000007105'
const husemannstr = '900000110511'
const when = new Date(Date.now() + 60 * MINUTE)

const hafas = createHafas('cached-hafas-client example')
const cachedHafas = withCache(hafas, store)

cachedHafas.on('hit', (method, ...args) => console.info('cache hit!', method, ...args))
cachedHafas.on('miss', (method, ...args) => console.info('cache miss!', method, ...args))

await cachedHafas.departures(wollinerStr, {
	duration: 10, when
})
const deps = await cachedHafas.departures(wollinerStr, {
	duration: 3, when: new Date(+when + 3 * MINUTE)
})
console.log(deps[0])

await cachedHafas.stop(wollinerStr)
const stop = await cachedHafas.stop(wollinerStr)
console.log(stop)

db.quit()
