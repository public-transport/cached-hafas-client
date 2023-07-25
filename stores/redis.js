// todo: use import assertions once they're supported by Node.js & ESLint
// https://github.com/tc39/proposal-import-assertions
import {createRequire} from 'module'
const require = createRequire(import.meta.url)

import {ok} from 'assert'
import {randomBytes} from 'crypto'
import createDebug from 'debug'
import commonPrefix from 'common-prefix'
import {NO_RESULTS} from '../no-results.js'
const pkg = require('../package.json')

const debug = createDebug('cached-hafas-client:redis')

const VERSION = pkg['cached-hafas-client'].dataVersion + ''
ok(VERSION)

const COLLECTIONS = 'c'
const COLLECTIONS_ROWS = 'r'
const ATOMS = 'a'

const READ_MATCHING_COLLECTION = `\
local collections_prefix = ARGV[1];
local rows_prefix = ARGV[2];
local created_min = tonumber(ARGV[3]); -- UNIX epoch in seconds
local created_max = tonumber(ARGV[4]); -- UNIX epoch in seconds
local when_min = tonumber(ARGV[5]); -- UNIX epoch in milliseconds
local when_max = tonumber(ARGV[6]); -- UNIX epoch in milliseconds

local function read_collection (id)
	local rows = {};

	redis.log(redis.LOG_DEBUG, 'scanning for collection rows (rows_prefix: ' .. rows_prefix .. ')');
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
				redis.log(redis.LOG_VERBOSE, 'collection row ' .. i .. ' matches');
				local row = redis.call("get", key);
				table.insert(rows, {i, row});
			else
				redis.log(redis.LOG_VERBOSE, 'collection row ' .. i .. ' doesn\\'t match (when: ' .. when .. ')');
			end
		end

		if cursor == "0" then
			redis.log(redis.LOG_VERBOSE, 'done scanning for collection rows');
			break
		end
	end

	return rows;
end

redis.log(redis.LOG_DEBUG, 'scanning for collections (collections_prefix: ' .. collections_prefix .. ')');
local cursor = "0";
while true do
	-- todo: scan in reverse order to, when in doubt, get the latest collection
	-- todo: COUNT 30 instead?
	local res = redis.call("scan", cursor, "match", collections_prefix .. "*", "COUNT", 100);
	cursor = res[1];

	for i, key in ipairs(res[2]) do
		local _, __, created = string.find(key, "[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)");
		created = tonumber(created);

		if created >= created_min and created <= created_max
		then
			local col = redis.call("get", key);
			local _, __, id, when, duration = string.find(col, "([^:]+):([^:]+):([^:]+)");
			redis.log(redis.LOG_VERBOSE, 'id: ' .. id .. 'when: ' .. when .. ' duration: ' .. duration);
			when = tonumber(when);
			duration = tonumber(duration);

			if when <= when_min and (when + duration) >= when_max
			then
				redis.log(redis.LOG_VERBOSE, 'collection ' .. id .. ' matches');
				return read_collection(id);
			else
				redis.log(redis.LOG_VERBOSE, 'collection ' .. id .. ' doesn\\'t match (when: ' .. when .. ' duration: ' .. duration .. ')');
			end
		else
			redis.log(redis.LOG_VERBOSE, 'collection ' .. id .. ' doesn\\'t match (created: ' .. created .. ')');
		end
	end

	if cursor == "0" then
		redis.log(redis.LOG_VERBOSE, 'done scanning for collections');
		break
	end
end

return nil;
`

const READ_MATCHING_ATOM = `\
local prefix = ARGV[1];
local created_min = tonumber(ARGV[2]); -- UNIX epoch in seconds
local created_max = tonumber(ARGV[3]); -- UNIX epoch in seconds

redis.log(redis.LOG_DEBUG, 'scanning for atoms (prefix: ' .. prefix .. ')');
local cursor = "0";
while true do
	-- todo: scan in reverse order to, when in doubt, get the latest atom
	local res = redis.call("scan", cursor, "match", prefix .. "*", "COUNT", 30);
	cursor = res[1];

	for i, key in ipairs(res[2]) do
		local _, __, created = string.find(key, "[^:]+:[^:]+:[^:]+:[^:]+:([^:]+)");
		created = tonumber(created);

		if created >= created_min and created <= created_max
		then
			local atom = redis.call("get", key);
			return atom;
		else
			redis.log(redis.LOG_VERBOSE, 'atom doesn\\'t match (created: ' .. created .. ')');
		end
	end

	if cursor == "0" then
		redis.log(redis.LOG_VERBOSE, 'done scanning for atoms');
		break
	end
end
`

const createRedisStore = (db) => {
	// todo: stop mutating `db`
	const _readMatchingCollection = 'readMatchingCollection' + VERSION
	if (!db[_readMatchingCollection]) {
		db.defineCommand(_readMatchingCollection, {
			numberOfKeys: 0,
			lua: READ_MATCHING_COLLECTION,
		})
	}
	const _readMatchingAtom = 'readMatchingAtom' + VERSION
	if (!db[_readMatchingAtom]) {
		db.defineCommand(_readMatchingAtom, {
			numberOfKeys: 0,
			lua: READ_MATCHING_ATOM,
		})
	}

	const init = async () => {
		debug('init')
	}

	const readCollection = async (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax
		} = args
		const createdMin = Math.floor(args.createdMin / 1000)
		const createdMax = Math.ceil(args.createdMax / 1000)

		const prefix = commonPrefix([
			[VERSION, COLLECTIONS, method, inputHash, createdMin].join(':'),
			[VERSION, COLLECTIONS, method, inputHash, createdMax].join(':')
		])
		const rowsPrefix = `${VERSION}:${COLLECTIONS_ROWS}:`
		const rows = await db[_readMatchingCollection](
			prefix, rowsPrefix,
			createdMin, createdMax,
			whenMin, whenMax,
		)
		
		if (rows === null) { // no matching collection found
			return NO_RESULTS
		}
		return rows
		.sort(([idxA], [idxB]) => idxA - idxB)
		.map(([_, data]) => ({data}))
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


		const cmds = [
			[
				'set',
				[VERSION, COLLECTIONS, method, inputHash, created].join(':'),
				[collectionId, when, duration].join(':'),
				'PX', cachePeriod,
			],
			...rows.map((row, i) => {
				// todo: fall back to plannedWhen?
				const t = +new Date(row.when)
				if (Number.isNaN(t)) throw new Error(`rows[${i}].when must be a number or an ISO 8601 string`)
				const key = [VERSION, COLLECTIONS_ROWS, collectionId, t, i].join(':')
				return ['set', key, row.data, 'PX', cachePeriod]
			}),
		]
		await db.multi(cmds).exec()
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
		const val = await db[_readMatchingAtom]([
			keysPrefix,
			createdMin, createdMax,
		])
		return val ? deserialize(val) : NO_RESULTS
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
		await db.set(key, serialize(val), 'PX', cachePeriod)
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
	createRedisStore,
}
