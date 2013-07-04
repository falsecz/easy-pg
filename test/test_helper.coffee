glob = require 'glob'
global.assert = require("chai").assert
{spawn} = require 'child_process'

glob "test/*.coffee", { cwd: __dirname + "/.."}, (err, files) ->
	console.log files.join ' '
