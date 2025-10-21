import { defineConfig } from 'eslint/config'
import js from '@eslint/js'
import globals from 'globals'

export default defineConfig([
	js.configs.recommended,
	{
		languageOptions: {
			ecmaVersion: 2023,
			sourceType: 'module',
			globals: {
				...globals.node,
			},
		},
		rules: {
			'no-unused-vars': 'off',
		},
	},
])
