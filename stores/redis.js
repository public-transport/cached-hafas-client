'use strict'

const {ok} = require('assert')
const {randomBytes} = require('crypto')
const debug = require('debug')('cached-hafas-client:redis')
const commonPrefix = require('common-prefix')
const pkg = require('../package.json')

const VERSION = pkg['cached-hafas-client'].dataVersion + ''
ok(VERSION)

const COLLECTIONS = 'c'
const COLLECTIONS_ROWS = 'r'
const ATOMS = 'a'

const READ_MATCHING_COLLECTION = `\
local collections_prefix = ARGV[1];
local rows_prefix = ARGV[2];
local created_min = tonumber(ARGV[3]);
local created_max = tonumber(ARGV[4]);
local when_min = tonumber(ARGV[5]);
local when_max = tonumber(ARGV[6]);

local function read_collection (id)
	local rows = {};

	local cursor = "0";
	while true do
		-- todo: pass in collection rows prefix
		local res = redis.call("scan", cursor, "match", rows_prefix .. id .. ":*", "COUNT", 30);
		cursor = res[1];

		for _, key in ipairs(res[2]) do
			local __, ___, when, i = string.find(key, "[^:]+:[^:]+:[^:]+:([^:]+):([^:]+)");
			when = tonumber(when);
			i = tonumber(i);

			if when >= when_min and when <= when_max
			then
				local row = redis.call("get", key);
				table.insert(rows, {i, row});
			end
		end

		if cursor == "0" then
			break
		end
	end

	return rows;
end

local cursor = "0";
while true do
	-- todo: scan in reverse order to, when in doubt, get the latest collection
	local res = redis.call("scan", cursor, "match", collections_prefix .. "*", "COUNT", 100);
	cursor = res[1];

	for i, key in ipairs(res[2]) do
		local _, __, created = string.find(key, "[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)");
		created = tonumber(created);

		if created >= created_min and created <= created_max
		then
			local col = redis.call("get", key);
			local _, __, id, when, duration = string.find(col, "([^:]+):([^:]+):([^:]+)");
			when = tonumber(when);
			duration = tonumber(duration);

			if when <= when_min and (when + duration) >= when_max
			then
				return read_collection(id);
			end
		end
	end

	if cursor == "0" then
		break
	end
end

return {};
`

const createStore = (db) => {
	// todo: stop mutating `db`
	if (!db.readMatchingCollection) {
		db.defineCommand('readMatchingCollection', {
			numberOfKeys: 0,
			lua: READ_MATCHING_COLLECTION,
		})
	}

	const init = async () => {
		debug('init')
	}

	const read = async (key) => {
		debug('read', key)
		return await db.get(key)
	}

	const write = async (key, val, ttl) => {
		debug('write', key, val.length, ttl)
		await db.set(key, val, 'PX', ttl)
	}

	const scanner = (pattern) => {
		debug('scanner', pattern)
		let cursor = '0', initial = true
		const iterate = async () => {
			if (!initial && cursor === '0') return {done: true, value: null}
			initial = false

			const res = await db.scan(cursor, 'MATCH', pattern, 'COUNT', '30')
			cursor = res[0]
			return {done: false, value: res[1]}
		}

		return {next: iterate}
	}
	const scan = pattern => ({
		[Symbol.asyncIterator]: () => scanner(pattern)
	})

	const readCollection = async (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax
		} = args
		const createdMin = Math.floor(args.createdMin / 1000)
		const createdMax = Math.ceil(args.createdMax / 1000)
		const rowToVal = args.rowToVal || (row => JSON.parse(row.data))

		const prefix = commonPrefix([
			[VERSION, COLLECTIONS, method, inputHash, createdMin].join(':'),
			[VERSION, COLLECTIONS, method, inputHash, createdMax].join(':')
		])
		const rowsPrefix = `${VERSION}:${COLLECTIONS_ROWS}:`
		const rows = await db.readMatchingCollection(
			prefix, rowsPrefix,
			createdMin, createdMax,
			whenMin, whenMax,
		)

		return rows
		.sort(([idxA], [idxB]) => idxA - idxB)
		.map(([idx, data]) => rowToVal({data}))
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash, when, duration,
			cachePeriod,
			rows
		} = args
		const created = Math.round(args.created / 1000)

		const collectionId = randomBytes(10).toString('hex')
		await write([
			VERSION, COLLECTIONS, method, inputHash, created
		].join(':'), [
			collectionId,
			when, duration
		].join(':'), cachePeriod)

		await Promise.all(rows.map(async (row, i) => {
			const t = +new Date(row.when)
			if (Number.isNaN(t)) throw new Error(`rows[${i}].when must be an ISO 8601 string`)
			const key = [VERSION, COLLECTIONS_ROWS, collectionId, t, i].join(':')
			await write(key, row.data, cachePeriod)
		}))
	}

	// atomics
	// method:inputHash:created:id
	// todo: this fails with `created` timestamps of different lengths (2033)

	const readAtom = async (args) => {
		debug('readAtom', args)
		const {
			method, inputHash
		} = args
		const createdMin = Math.floor(args.createdMin / 1000)
		const createdMax = Math.ceil(args.createdMax / 1000)
		const deserialize = args.deserialize || JSON.parse

		const keysPrefix = commonPrefix([
			[VERSION, ATOMS, method, inputHash, createdMin].join(':'),
			[VERSION, ATOMS, method, inputHash, createdMax].join(':')
		])

		// todo: scan in reverse order to, when in doubt, get the latest item
		for await (const keys of scan(keysPrefix + '*')) {
			for (let key of keys) {
				const created = parseInt(key.split(':')[4])
				if (Number.isNaN(created) || created < createdMin || created > createdMax) continue

				const val = await read(key)
				return deserialize(val)
			}

		}
		return null
	}

	const writeAtom = async (args) => {
		debug('writeAtom', args)
		const {
			method, inputHash,
			cachePeriod,
			val
		} = args
		const created = Math.round(args.created / 1000)
		const serialize = args.serialize || JSON.stringify

		const key = [VERSION, ATOMS, method, inputHash, created].join(':')
		await write(key, serialize(val), cachePeriod)
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
