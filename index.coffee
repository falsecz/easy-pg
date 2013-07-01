debug = require('debug') 'easy-pg'

setImmediate = setImmediate ? process.nextTick
module.exports = (connString, options) ->
	return new Db connString, options

# class Db
# util = require 'util'
pg = require "pg" # . native TODO
{EventEmitter} = require "events"
{QueryObject} = require "./queryObject.coffee"
# colors = require 'colors'
# url = require 'url'

class Db extends EventEmitter

	# needs connection string for postgresql
	# later probably just options object
	# options.lazy = false makes db connection star right now
	constructor: (connString, options={}) ->
		@connectionString = connString #client's connection string
		@state = "offline" #client's connection state
		@queue = []  #client's queue for queries

		@tryToConnect() if options.lazy is false


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

		#create and connect new psql client

	###
	Tries to connect to DB. If connection drops or some
	error occures, tries it later
	###
	tryToConnect: () =>
		@state = "connecting"
		@client = new pg.Client @connectionString
		@client.connect (err, client) =>
			if err #register request for later connection
				@emit "error", err.toString() #new Error "connection failed ..."
				setTimeout @tryToConnect, 2000
			else
				client.on "error", (err) => #try to connect again immediately
					@emit "error", err.toString() #new Error "connection lost..."
					@tryToConnect()

				@state = "online"
				@emit "ready"
				@queuePull()   #process first query in the queue immediately

	###
	Returns true if the given postgresql error code
	is acceptable = there is no reason to remove query
	with this code from our queue or to throw error
	###
	acceptable = (code) ->
		return no unless code?
		errClass = code.slice(0, 2) #first two signs describe error class
		# console.log "err class: " + errClass
		code in [
			"00"  # Successful Completion
			"08"# Connection Exception
			"57"  # Operator Intervention
		]

	###
	Pushes given input into queue for later dispatching
	requires "type" and "query"
	###
	queuePush: (type, query, values, done) =>
		#for the case of calling (type, query, done)
		if typeof values is "function"
			done = values
			values = null

		#create object with special callback to remove query from queue when it is processed
		qObj = new QueryObject type, query, values, (err, result) =>
			debug "callBack"
			#remove first querry from queue (it is processed now)
			#if it is OK or error is on our side
			console.log "acceptable code: "+acceptable err.code if err?
			unless acceptable err?.code
				@queue.shift()
				done? err, result

			else #try the failed query with acceptable code again
				@printQueue()
				console.log @state, @client

			@queuePull()   #process next in the queue

		@queue.push qObj #register another query
		@queuePull() if @queue.length is 1 #process first query in the queue


	###
	Dispatches first query in the queue if its not empty
	Tries to connect to db if the client state is "offline"
	###
	queuePull: () =>
		debug "queuePull"
		if @queue.length > 0
			@queue[0].callBy @client if @state is "online"
			@tryToConnect() if @state is "offline"


	printQueue: () =>
		console.log "---------------------------"
		console.log q.toString() for q in @queue
		console.log "---------------------------"


	###
	Returns full DB result with data in the ".rows" entry
	requires just "query"
	###
	query: (query, values, done) =>

		@queuePush "QueryRaw", query, values, done


	###
	Returns null or only the first row of the original DB result
	requires just "query"
	###
	queryOne: (query, values, done) =>

		@queuePush "QueryOne", query, values, done


	###
	Returns null or array of rows of the original DB result
	requires just "query"
	###
	queryAll: (query, values, done) =>

		@queuePush "QueryAll", query, values, done


	###
	Inserts "data" into specified "table"
	requires "table" and "data"
	###
	insert: (table, data, done) =>
		keys = []
		valIds = []
		values = []
		i = 1

		for key, val of data
			keys.push key
			values.push val
			valIds.push "$#{i++}"

		query = "INSERT INTO #{table} (#{keys.join ', '}) VALUES (#{valIds.join ', '}) RETURNING *"
		@queryOne query, values, done


	###
	Updates specified "table" using given "data"
	requires "table", "data", "where", "done"
	###
	update: (table, data, where, whereData, done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

			keys = []
			valIds = []
			values = []
			i = 1

		sets = []
		for key, val of data
			sets.push "#{key} = $#{i++}"
			values.push val

		where = where.replace /\$(\d+)/g, (match, id) ->
			"$" + (i - 1 + parseInt(id))

		for val in whereData
			values.push val

		query = "UPDATE #{table} SET #{sets.join ', '} WHERE #{where} RETURNING *"

		@queryOne query, values, done


	###
	Starts a transaction block
	###
	begin: (done) =>

		@query "BEGIN", done


	###
	Commits current transaction
	###
	commit: (done) =>

		@query "COMMIT", done

	###
	Aborts current transaction
	###
	rollback: (done) =>

		@query "ROLLBACK", done


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

