'use strict'

const DEBUG = process.env.NODE_DEBUG === 'cached-hafas-client'

const createHafas = require('vbb-hafas')
const {DateTime} = require('luxon')
const sqlite3 = DEBUG ? require('sqlite3').verbose() : require('sqlite3')
const Redis = require('ioredis')

const hafas = createHafas('cached-hafas-client test')

const {timezone, locale} = hafas.profile
const when = new Date(DateTime.fromMillis(Date.now(), {
	zone: timezone, locale,
})
.startOf('week').plus({weeks: 1, hours: 10})
.toISO())

const createSpy = (origFn) => {
	const spyFn = (...args) => {
		spyFn.callCount++
		return origFn.apply({}, args)
	}
	spyFn.callCount = 0
	return spyFn
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms, null))

const createSqliteDb = () => {
	const db = new sqlite3.Database(':memory:')
	if (DEBUG) db.on('profile', query => console.debug(query))
	const teardown = () => {
		db.close()
		return Promise.resolve()
	}
	return Promise.resolve({db, teardown})
}

const createRedisDb = () => {
	const db = new Redis()
	const teardown = () => {
		return new Promise((resolve, reject) => {
			db.flushdb((err) => {
				if (err) return reject(err)
				db.quit()
				resolve()
			})
		})
	}
	return Promise.resolve({db, teardown})
}

module.exports = {
	hafas,
	when,
	createSpy,
	delay,
	createSqliteDb,
	createRedisDb
}
