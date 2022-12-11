// todo: use import assertions once they're supported by Node.js & ESLint
// https://github.com/tc39/proposal-import-assertions
import {createRequire} from 'module'
const require = createRequire(import.meta.url)

import createDebug from 'debug'
import LRUCache from 'quick-lru'
import commonPrefix from 'common-prefix'
import clone from 'shallow-clone'
const pkg = require('../package.json')

const debug = createDebug('cached-hafas-client:in-memory')

const COLLECTIONS = 'c'
const ATOMS = 'a'

const createInMemoryStore = (opt = {}) => {
	const {
		maxSize,
	} = {
		maxSize: 1000,
		...opt,
	}

	const lru = new LRUCache({maxSize})

	const init = async () => {
	}

	const readCollection = async (args) => {
		debug('readCollection', args)
		const {
			method, inputHash, whenMin, whenMax,
		} = args
		const createdMin = Math.floor(args.createdMin / 1000)
		const createdMax = Math.ceil(args.createdMax / 1000)

		// todo: make sure to always get the latest collection
		const keyPrefix = [COLLECTIONS, method, inputHash, ''].join(':')
		for (const [key, entry] of lru) {
			if (key.slice(0, keyPrefix.length) !== keyPrefix) continue

			// is it too old?
			const [created, ...rows] = entry
			if (created < createdMin || created > createdMax) continue

			// is the time frame covered?
			const _whenMin = parseInt(key.split(':')[3])
			if (_whenMin > Math.floor(whenMin / 1000)) continue
			const _whenMax = parseInt(key.split(':')[4])
			if (_whenMax < Math.ceil(whenMax / 1000)) continue

			lru.get(key) // mark entry as used
			return rows
			.filter(r => r.when >= whenMin && r.when <= whenMax)
		}

		return []
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash, when, duration,
			rows,
		} = args
		const whenMin = Math.floor(when / 1000)
		const whenMax = Math.ceil((when + duration) / 1000)
		const created = Math.round(args.created / 1000)

		const key = [COLLECTIONS, method, inputHash, whenMin, whenMax].join(':')
		const entry = [created, ...rows] // rows[].data are already serialized
		lru.set(key, entry)
	}

	const readAtom = async (args) => {
		debug('readAtom', args)
		const {
			method, inputHash,
		} = args
		const createdMin = Math.round(args.createdMin / 1000)
		const createdMax = Math.round(args.createdMax / 1000)
		const deserialize = args.deserialize || (val => val)

		const key = [ATOMS, method, inputHash].join(':')
		if (!lru.has(key)) return null
		const [created, val] = lru.get(key)
		if (created < createdMin || created > createdMax) return null
		return deserialize(val)
	}

	const writeAtom = async (args) => {
		debug('writeAtom', args)
		const {
			method, inputHash,
			cachePeriod,
			val,
		} = args
		const created = Math.round(args.created / 1000)
		const serialize = args.serialize || (val => clone(val))

		const key = [ATOMS, method, inputHash].join(':')
		lru.set(key, [created, serialize(val)])
	}

	return {
		init,
		readCollection,
		writeCollection,
		readAtom,
		writeAtom
	}
}

export {
	createInMemoryStore,
}
