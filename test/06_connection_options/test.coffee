async = require "async"
pg = require "../../"

unless connectionStr = process.argv[2]
	console.error 'Missing connection string'
	process.exit 1

unless schemaName = process.argv[3]
	console.error 'Missing schema name'
	process.exit 1

async.series [
	(next) ->
		pg(connectionStr).query "SELECT 1", next

	(next) ->
		async.times 3, (i, next) ->
			pg(connectionStr).queryAll "SELECT 1", next
		, next

	(next) ->
		db = pg(connectionStr)
		async.timesSeries 3, (i, next) ->
			db.queryOne 'SHOW search_path', (e, r) ->
				return next e if e
				searchPath = r.search_path
				return next "Expected search_path '#{searchPath}' to be '#{schemaName}' (query ##{i+1})" if searchPath isnt schemaName
				next()
		, next
], (e) ->
	console.error e if e
	process.exit Number(e?)
