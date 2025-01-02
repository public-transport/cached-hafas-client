import {writeFileSync} from 'node:fs'
import {fileURLToPath} from 'node:url'
import {
	READ_MATCHING_COLLECTION,
	READ_MATCHING_ATOM,
} from '../stores/redis.js'

writeFileSync(
	fileURLToPath(new URL('./read-matching-collection.lua', import.meta.url)),
	READ_MATCHING_COLLECTION,
)
writeFileSync(
	fileURLToPath(new URL('./read-matching-atom.lua', import.meta.url)),
	READ_MATCHING_ATOM,
)
