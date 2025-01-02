#!/bin/bash

set -euo pipefail
cd "$(dirname "$0")"
set -x

node extract-lua-code.js

luacheck *.lua
