'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')
const commonPrefix = require('common-prefix')

const TTL = 60 * 5 // todo: make customizable

const createStore = (db) => {
	const init = (cb) => {
		setImmediate(cb, null)
	}

	const writeWithTtl = (key, val, ttl) => {
		return new Promise((resolve, reject) => {
			db.set(key, val, (err) => {
				if (err) return reject(err)
				db.expire(key, TTL, (err) => {
					if (err) reject(err)
					else resolve()
				})
			})
		})
	}

	// todo [breaking]: use async iteration
	const scan = (pattern, cb) => {
		let cursor = '0'
		const iterate = () => {
			db.scan(cursor, 'MATCH', pattern, 'COUNT', '30', (err, res) => {
				if (err) return cb(err)
				cursor = res[0]

				const abort = cb(null, res[1])
				if (abort === true) return cb(null)

				if (cursor === '0') return cb(null)
				iterate()
			})
		}
		iterate()
	}

	const readCollection = (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax,
			createdMin, createdMax
		} = args
		const rowToVal = args.rowToVal || (row => JSON.parse(row.data))

		// todo
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash,
			when, duration,
			rows
		} = args
		const created = Math.round(args.created / 1000)

		// todo
	}

	// atomics
	// method:inputHash:created:id
	// todo: this fails with `created` timestamps of different lengths (2033)

	const readAtom = (method, inputHash, createdMin, createdMax, deserialize) => {
		debug('readAtom', {method, inputHash, createdMin, createdMax, deserialize})
		createdMin = Math.floor(createdMin / 1000)
		createdMax = Math.ceil(createdMax / 1000)
		deserialize = deserialize || JSON.parse

		const keysPrefix = commonPrefix([
			[method, inputHash, createdMin].join(':') + ':',
			[method, inputHash, createdMax].join(':') + ':'
		])

		return new Promise((resolve, reject) => {
			// todo: scan in reverse order to, when in doubt, get the latest item
			scan(keysPrefix + '*', (err, res) => {
				if (err) return reject(err)
				if (!res) return resolve(null)

				for (let key of res) {
					const created = parseInt(key.split(':')[2])
					if (Number.isNaN(created) || created < createdMin || created > createdMax) continue

					resolve(key)
					return true // abort scanning
				}
			})
		})
		.then((key) => {
			if (!key) return null
			return new Promise((resolve, reject) => {
				db.get(key, (err, val) => {
					if (err) return reject(err)
					try {
						resolve(deserialize(val))
					} catch (err) {
						reject(err)
					}
				})
			})
		})
	}

	const writeAtom = (method, inputHash, created, val, serialize) => {
		debug('writeAtom', {method, inputHash, created, val, serialize})
		created = Math.round(created / 1000)
		serialize = serialize || JSON.stringify

		const id = randomBytes(4).toString('hex')
		const key = [method, inputHash, created, id].join(':')
		return writeWithTtl(key, serialize(val), TTL)
	}

	return {
		init,
		readCollection,
		writeCollection,
		readAtom,
		writeAtom
	}
}

module.exports = createStore
