setImmediate = setImmediate ? process.nextTick
module.exports = (connString) ->
	return new Db


# class Db
# util = require 'util'
# pg = require('pg') # . native TODO
{EventEmitter} = require 'events'
# colors = require 'colors'
# url = require 'url'

class Db extends EventEmitter

	constructor: () ->


		setImmediate =>
			@emit 'error', new Error "couldn't connect to ..."

		# @TODO setdate style
		# 	@query """ SET datestyle = "iso, mdy" """

		# opts =
		# 	poolSize: 5
		# 	# user: ''
		# 	# host: '.dev1.nag.ccl'
		# 	# port: 5432
		# 	# database: ''
		# 	# password: ''
		# 	poolLog: console.log
		#
		# result = url.parse process.config.pgConnString
		# opts.host = result.hostname;
		# opts.database = if result.pathname then result.pathname.slice(1) else null
		# auth = (result.auth || ':').split(':')
		# opts.user = auth[0]
		# opts.password = auth[1]
		# opts.port = result.port
		#
		#
		#
		# pg.connect opts, (err, client) =>
		# 	@client = client
		# 	@query """ SET datestyle = "iso, mdy" """
		# 	# @query """ set search_path public """
		#
		# 	console.log 'QB initialized'
		# 	done()
		# 	# GLOBAL.__CLIENT = client
		#

		# @client.connect()

	queryOne: (query, values, done) =>
		# @query query, values, (err, result) ->
		# 	result = result?.rows?[0]
		# 	result ?= null
		# 	done? err, result

	queryAll: (query, values, done) ->
		# console.log query, values
		# @query query, values, (err, result) ->
		# 	done? err, result?.rows


	paginate: (offset, limit, cols, query, values, done) =>
		offset = parseInt offset
		limit = parseInt limit
		console.log 'ppapapapapapappa'
		countQuery = "SELECT COUNT(#{cols}) FROM #{query}"
		dataQuery = "SELECT #{cols} FROM #{query} OFFSET #{offset} LIMIT #{limit}"

		@queryOne countQuery, values, (err, count) =>
			return done err if err
			@queryAll dataQuery, values, (err, result) =>
				return done err if err

				o =
					totalCount: count.count
					currentOffset: offset
					nextOffset: offset + limit
					previousOffset: offset - limit

					data: result

				o.previousOffset = null if o.previousOffset < 0

				o.nextOffset = null if o.nextOffset > o.totalCount



				return done null, o







	query: (query, values, done) ->
		# done = values unless values
		#
		# # console.log query.yellow
		# @client.query query, values, (err, result) =>
		# 	console.log "DONE " +  query.yellow + " " + values
		# 	if err
		# 		# @emit 'error',
		# 		# 	err: err
		# 		# 	query: query
		# 		# 	values: values
		#
		# 		util.log "#{query}".red
		# 		util.log "#{values}".red
		# 		util.log "#{err}".red
		# 		err =
		# 			message: "#{err}"
		# 			values: "#{values}"
		# 			query: "#{query}"
		#
		# 	# util.log util.inspect result.rows
		# 	return done? err, result

	insert: (table, data, done) ->
		# keys = []
		# valIds = []
		# values = []
		# i = 1
		#
		# for key, val of data
		# 	keys.push key
		# 	values.push val
		# 	valIds.push "$#{i++}"
		#
		#
		# q = "INSERT INTO #{table} (#{keys.join ', '}) VALUES (#{valIds.join ', '}) RETURNING *"
		#
		# @queryOne q, values, done

	update: (table, data, where, whereData, done) ->
		# if typeof whereData is 'function'
	# 		done = whereData
	# 		whereData = []
	#
	# 	keys = []
	# 	valIds = []
	# 	values = []
	# 	i = 1
	#
	# 	sets = []
	# 	for key, val of data
	# 		sets.push "#{key} = $#{i++}"
	# 		values.push val
	#
	# 	where = where.replace /\$(\d+)/g, (match, id) ->
	# 		"$" + (i - 1 + parseInt(id))
	#
	# 	for val in whereData
	# 		values.push val
	#
	# 	q = "UPDATE #{table} SET #{sets.join ', '} WHERE #{where} RETURNING *"
	#
	# 	@queryOne q, values, done
	#
	# upsert: (table, data, where, whereData, done) ->
	# 	update = no
	# 	if Array.isArray whereData
	# 		update = yes
	# 		for d in whereData
	# 			update = no unless d?
	# 			break
	#
	# 	if update
	# 		@update table, data, where, whereData, done
	# 	else
	# 		@insert table, data, done
	#
	upsert: (table, data, where, whereData, done) ->
		# console.log 'Where data'.log
	# 	util.log util.inspect whereData
	# 	@queryOne "SELECT COUNT(*) FROM #{table} WHERE #{where}", whereData, (err, found) =>
	# 		done err if err
	# 		if found.count
	# 			@update table, data, where, whereData, done
	# 		else
	# 			@insert table, data, done

		# update = no
		# if Array.isArray whereData
		# 	update = yes
		# 	for d in whereData
		# 		update = no unless d?
		# 		break
		#
		# if update
		# 	@update table, data, where, whereData, done
		# else
		# 	@insert table, data, done

	begin: (done) ->
		# @query "BEGIN", [], done

	commit: (done) ->
		# @query "COMMIT", [], done

	rollback: (done) ->
		# @query "ROLLBACK", [], done

