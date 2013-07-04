glob = require 'glob'
global.assert = require("chai").assert

glob "test/*.coffee", { cwd: __dirname + "/.."}, (err, files) ->
	console.log files.join ' '
