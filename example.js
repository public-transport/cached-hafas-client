'use strict'

const createHafas = require('vbb-hafas')
const sqlite3 = require('sqlite3')
const withCache = require('.')
const createSqliteStore = require('./stores/sqlite')

const hafas = createHafas('hafas-client-cache example')
const db = new sqlite3.Database(':memory:')
const store = createSqliteStore(db)
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
	.then((deps) => {
		console.log(deps[0])
		db.close()
	})
	.catch(onError)

	cachedHafas.journeys(wollinerStr, husemannstr, {results: 2, departure: when})
	.then(() => cachedHafas.journeys(wollinerStr, husemannstr, {results: 2, departure: when}))
	.then((journeys) => {
		console.log(journeys[0])
		db.close()
	})
	.catch(onError)
})

const onError = (err) => {
	console.error(err)
	process.exit(1)
}
