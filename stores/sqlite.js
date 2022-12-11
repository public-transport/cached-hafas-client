// todo: use import assertions once they're supported by Node.js & ESLint
// https://github.com/tc39/proposal-import-assertions
import {createRequire} from 'module'
const require = createRequire(import.meta.url)

import {ok} from 'assert'
import {promisify} from 'util'
import {randomBytes} from 'crypto'
import createDebug from 'debug'
const pkg = require('../package.json')

const debug = createDebug('cached-hafas-client:sqlite')

const V = pkg['cached-hafas-client'].dataVersion + ''
ok(V)

const CREATE_COLLECTION_QUERIES_TABLE = `\
CREATE TABLE IF NOT EXISTS collection_queries_${V} (
	collection_queries_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	"when" INT NOT NULL,
	duration INT NOT NULL,
	inputHash CHARACTER(32) NOT NULL
);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_created_idx
	ON collection_queries_${V} (created);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_method_idx
	ON collection_queries_${V} (method);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_when_idx
	ON collection_queries_${V} ("when");
CREATE INDEX IF NOT EXISTS collection_queries_${V}_duration_idx
	ON collection_queries_${V} (duration);
CREATE INDEX IF NOT EXISTS collection_queries_${V}_inputHash_idx
	ON collection_queries_${V} (inputHash);
`

const CREATE_COLLECTIONS_TABLE = `\
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS collections_${V} (
	collections_id CHARACTER(20) PRIMARY KEY,
	query_id CHARACTER(20) NOT NULL,
	i INT,
	"when" INT,
	data TEXT NOT NULL,
	FOREIGN KEY (query_id) REFERENCES collection_queries_${V}(collection_queries_id)
);
CREATE INDEX IF NOT EXISTS collections_${V}_query_id_idx
	ON collections_${V} (query_id);
`

const READ_COLLECTIONS = `\
SELECT collections_${V}.data FROM collection_queries_${V}
LEFT JOIN collections_${V}
	ON collection_queries_${V}.collection_queries_id = collections_${V}.query_id
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
	-- find queries that cover the when -> (when + duration) period
	AND collection_queries_${V}."when" <= $whenMin
	AND (collection_queries_${V}."when" + duration) >= $whenMax
	-- only get items in the when -> (when + duration) period
	AND collections_${V}."when" >= $whenMin
	AND collections_${V}."when" <= $whenMax
ORDER BY collections_${V}.i ASC;
`

const WRITE_COLLECTION_QUERY = `\
INSERT OR REPLACE INTO collection_queries_${V}
(collection_queries_id, method, created, "when", duration, inputHash)
VALUES ($id, $method, $created, $when, $duration, $inputHash);
`

const WRITE_COLLECTIONS = `\
INSERT INTO collections_${V}
(collections_id, query_id, i, "when", data)
VALUES ($id, $queryId, $i, $when, $data);
`

// "atom queries": Queries whose return values can only be cached together.
// Example: Caching 1 of 3 journeys and reusing for other queries is impossible.

const CREATE_ATOMS_TABLE = `\
CREATE TABLE IF NOT EXISTS atoms_${V} (
	atoms_id CHARACTER(20) PRIMARY KEY,
	created INT NOT NULL,
	method VARCHAR(12) NOT NULL,
	inputHash CHARACTER(32) NOT NULL,
	data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS atoms_${V}_created_idx
	ON atoms_${V} (created);
CREATE INDEX IF NOT EXISTS atoms_${V}_inputHash_idx
	ON atoms_${V} (inputHash);
CREATE INDEX IF NOT EXISTS atoms_${V}_data_idx
	ON atoms_${V} (data);
`

const READ_ATOM = `\
SELECT data FROM atoms_${V}
WHERE
	-- only find equal queries
	method = $method
	AND inputHash = $inputHash
	-- find queries created within the cache period
	AND created >= $createdMin
	AND created <= $createdMax
LIMIT 1;
`

const WRITE_ATOM = `\
INSERT OR REPLACE INTO atoms_${V}
(atoms_id, created, method, inputHash, data)
VALUES ($id, $created, $method, $inputHash, $data);
`

const createSqliteStore = (db) => {
	const dbExec = promisify(db.exec.bind(db))
	const dbAll = promisify(db.all.bind(db))
	const dbRun = promisify(db.run.bind(db))
	const dbGet = promisify(db.get.bind(db))

	const init = async (cb) => {
		debug('init')
		await dbExec([
			CREATE_COLLECTION_QUERIES_TABLE,
			CREATE_COLLECTIONS_TABLE,
			CREATE_ATOMS_TABLE
		].join('\n'))
	}

	const readCollection = async (args) => {
		debug('readCollection', args)
		const {
			method, inputHash,
			whenMin, whenMax,
			createdMin, createdMax
		} = args

		const rows = await dbAll(READ_COLLECTIONS, {
			'$method': method, // 'dep' or 'arr'
			'$inputHash': inputHash,
			'$createdMin': Math.floor(createdMin / 1000),
			'$createdMax': Math.ceil(createdMax / 1000),
			'$whenMin': whenMin / 1000 | 0,
			'$whenMax': whenMax / 1000 | 0
		})
		// todo: expose `.created`
		return rows
	}

	const writeCollection = async (args) => {
		debug('writeCollection', args)
		const {
			method, inputHash,
			when, duration,
			created, rows
		} = args
		const queryId = randomBytes(10).toString('hex')

		await dbRun(WRITE_COLLECTION_QUERY, {
			'$id': queryId,
			'$method': method, // 'dep' or 'arr'
			'$created': created / 1000 | 0,
			'$when': when / 1000 | 0,
			'$duration': duration / 1000 | 0,
			'$inputHash': inputHash
		})

		// todo: use `cmd = db.prepare; cmd.bind` for performance!
		// const cmd = db.prepare(WRITE_COLLECTIONS)
		for (let i = 0; i < rows.length; i++) {
			const row = rows[i]

			await dbRun(WRITE_COLLECTIONS, {
				'$id': randomBytes(10).toString('hex'),
				'$queryId': queryId,
				'$i': i,
				'$when': new Date(row.when) / 1000 | 0, // todo
				'$data': row.data
			})
		}
		// await new cmd.finalize()
	}

	const readAtom = async (args) => {
		debug('readAtom', args)
		const {
			method, inputHash,
			createdMin, createdMax
		} = args
		const deserialize = args.deserialize || JSON.parse

		const row = await dbGet(READ_ATOM, {
			'$method': method,
			'$inputHash': inputHash,
			'$createdMin': Math.floor(createdMin / 1000),
			'$createdMax': Math.ceil(createdMax / 1000)
		})

		if (!row || !row.data) return null
		return deserialize(row.data)
	}

	const writeAtom = async (args) => {
		debug('writeAtom', args)
		const {
			method, inputHash,
			created,
			val
		} = args
		const serialize = args.serialize || JSON.stringify

		await dbRun(WRITE_ATOM, {
			'$id': randomBytes(10).toString('hex'),
			'$created': created / 1000 | 0,
			'$method': method,
			'$inputHash': inputHash,
			'$data': serialize(val)
		})
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
	createSqliteStore,
}
