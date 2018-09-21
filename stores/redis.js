'use strict'

const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client')
const commonPrefix = require('common-prefix')

const VERSION = '1'

const COLLECTIONS = 'c'
const COLLECTIONS_ROWS = 'r'
const ATOMS = 'a'

const TTL = 60 * 5 // todo: make customizable

const createStore = (db) => {
	const init = (cb) => {
		setImmediate(cb, null)
	}

	const read = (key) => {
		return new Promise((resolve, reject) => {
			db.get(key, (err, val) => {
				if (err) reject(err)
				else resolve(val)
			})
		})
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

	// todo [breaking]: use async iteration using `Symbol.asyncIterator`
	const scan = (pattern) => {
		let cursor = '0'
		const iterate = () => {
			return new Promise((resolve, reject) => {
				db.scan(cursor, 'MATCH', pattern, 'COUNT', '30', (err, res) => {
					if (err) return reject(err)
					cursor = res[0]
					resolve({done: cursor === '0', value: res[1]})
				})
			})
		}

		return {next: iterate}
	}

	const findMatchingCollection = async (args) => {
		const {
			method, inputHash,
			whenMin, whenMax
		} = args
		const createdMin = Math.floor(args.createdMin / 1000)
		const createdMax = Math.ceil(args.createdMax / 1000)

		// todo: scan in reverse order to, when in doubt, get the latest collection
		const scanner = scan(commonPrefix([
			[VERSION, COLLECTIONS, method, inputHash, createdMin].join(':'),
			[VERSION, COLLECTIONS, method, inputHash, createdMax].join(':')
		]) + '*')
		while (true) {
			const {done, value: keys} = await scanner.next()

			for (let key of keys) {
				const created = parseInt(key.split(':')[4])
				if (Number.isNaN(created) || created < createdMin || created > createdMax) continue

				const {when, duration, id} = JSON.parse(await read(key))
				if (when <= whenMin && (when + duration) >= whenMax) return id
			}

			if (done) return null
		}
	}

	const readCollection = async (args) => {
		debug('readCollection', args)
		const whenMin = Math.floor(args.whenMin / 1000)
		const whenMax = Math.ceil(args.whenMax / 1000)
		const rowToVal = args.rowToVal || (row => JSON.parse(row.data))

		const id = await findMatchingCollection(args)
		if (!id) return []

		const prefix = [VERSION, COLLECTIONS_ROWS, id].join(':') + ':'
		const scanner = scan(prefix + '*')
		const rows = []
		while (true) {
			const {done, value: keys} = await scanner.next()

			for (let key of keys) {
				const keyParts = key.split(':')

				const when = parseInt(keyParts[3])
				if (Number.isNaN(when) || when < whenMin || when > whenMax) continue

				const data = await read(key)
				rows.push([
					parseInt(keyParts[4]), // i
					rowToVal({data})
				])
			}

			if (done) return rows.sort((a, b) => a[0] - b[0]).map(row => row[1])
		}
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash,
			when, duration,
			rows
		} = args
		const created = Math.round(args.created / 1000)

		const collectionId = randomBytes(10).toString('hex')
		await writeWithTtl([
			VERSION, COLLECTIONS, method, inputHash, created
		].join(':'), JSON.stringify({
			id: collectionId,
			when, duration
		}), TTL)

		await Promise.all(rows.map((row, i) => {
			const t = Math.round(new Date(row.when) / 1000)
			if (Number.isNaN(t)) throw new Error(`rows[${i}].when must be an ISO 8601 string`)
			const key = [VERSION, COLLECTIONS_ROWS, collectionId, t, i].join(':')
			return writeWithTtl(key, row.data, TTL)
		}))
	}

	// atomics
	// method:inputHash:created:id
	// todo: this fails with `created` timestamps of different lengths (2033)

	const readAtom = async (method, inputHash, createdMin, createdMax, deserialize) => {
		debug('readAtom', {method, inputHash, createdMin, createdMax, deserialize})
		createdMin = Math.floor(createdMin / 1000)
		createdMax = Math.ceil(createdMax / 1000)
		deserialize = deserialize || JSON.parse

		const keysPrefix = commonPrefix([
			[VERSION, ATOMS, method, inputHash, createdMin].join(':'),
			[VERSION, ATOMS, method, inputHash, createdMax].join(':')
		])

		// todo: scan in reverse order to, when in doubt, get the latest item
		const scanner = scan(keysPrefix + '*')
		while (true) {
			const {done, value: keys} = await scanner.next()

			for (let key of keys) {
				const created = parseInt(key.split(':')[4])
				if (Number.isNaN(created) || created < createdMin || created > createdMax) continue

				const val = await read(key)
				return deserialize(val)
			}

			if (done) return null
		}
	}

	const writeAtom = (method, inputHash, created, val, serialize) => {
		debug('writeAtom', {method, inputHash, created, val, serialize})
		created = Math.round(created / 1000)
		serialize = serialize || JSON.stringify

		const key = [VERSION, ATOMS, method, inputHash, created].join(':')
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
