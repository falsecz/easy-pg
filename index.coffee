debug = require('debug') 'easy-pg'
pg = require "pg" # . native TODO

{EventEmitter} = require "events"
{QueryObject} = require "./queryObject.coffee"

# needed to be even able to catch "missing conn params"
setImmediate = setImmediate ? process.nextTick


###
 Deferred Postgresql Client Class

 Contains queue of queries processed only in the case of established
 connection. Connection is established only after the first query is
 inserted into the client's queue, if options.lazy is set to "true".
###
class Db extends EventEmitter

	# connection parameters required by this client
	requiredConnParams = ["user", "pswd", "host", "port", "db"]


	# Constructor of the deferred postgresql client
	# requires "conn" object with connection parameters, but it
	# is possible to replace "conn" object by connection string
	# opts.lazy = false makes db connection star immediately
	constructor: (conn, opts={}) ->

		if typeof conn is "object"
			# check connection parameters
			for param in requiredConnParams
				setImmediate => # to be even able to catch this error
					return @emit("error", new Error "#{param} missing in connection parameters") unless (param of conn)

			#create connection string for pg
			connString = "pg://#{conn.user}:#{conn.pswd}@#{conn.host}:#{conn.port}/#{conn.db}"

		else connString = conn #just use the given connection string

		@connectionString = connString #client's connection string
		@state = "offline"             #client's connection state
		@queue = []                    #client's queue for queries

		# theres nothing more to do if options does not exist
		return unless opts?

		@tryToConnect() if opts.lazy is no

		# ONLY IF SET IN OPTS
		# 	@query """ SET datestyle = "iso, mdy" """
		# 	@query """ set search_path public """


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
				GLOBAL.__DPGCLIENT = client
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
		errClass in [
			"00"  # Successful Completion
			"08"  # Connection Exception
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
			#remove first querry from queue (it is processed now)
			#if it is OK or error is on our side
			unless acceptable err?.code
				@queuePull(true)
				done? err, result

			else #try the failed query with acceptable code again
				 #unfortunately, it is necessary to reconnect
				@state = "offline"
				@queuePull() #process next in the queue

		@queue.push qObj #register another query
		@queuePull() if @queue.length is 1 #process first query in the queue


	###
	 Dispatches first query in the queue if its not empty
	 Tries to connect to db if the client state is "offline"
	###
	queuePull: (shiftFirst = false) =>
		@queue.shift() if shiftFirst #shift solved here to make this Pull ALMOST ATOMIC

		if @queue.length > 0
			@queue[0].callBy @client if @state is "online"
			@tryToConnect() if @state is "offline"


	###
	 Just for debug, prints queue content
	###
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
	update: (table, data, where, whereData=[], done) =>
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
	 Updates (inserts) data in the specified "table"
	 requires "table", "data", "where", "whereData", "done"
	###
	upsert: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		@queryOne "SELECT COUNT(*) FROM #{table} WHERE #{where}", whereData, (err, found) =>
			return done err if err

			if found.count > 0
				@update table, data, where, whereData, done
			else
				@insert table, data, done


	###
	 Returns paginated "query" result containing max "limit" rows
	 requires "offset", "limit", "cols", "query", "values", "done"
	###
	paginate: (offset, limit, cols, query, values, done) =>
		offset = parseInt offset
		limit = parseInt limit
		countQuery = "SELECT COUNT(#{cols}) FROM #{query}"
		dataQuery = "SELECT #{cols} FROM #{query} OFFSET #{offset} LIMIT #{limit}"

		@queryOne countQuery, values, (err, count) =>
			return done err if err
			@queryAll dataQuery, values, (err, result) =>
				return done err if err

				res =
					totalCount: count.count
					currentOffset: offset
					nextOffset: offset + limit
					previousOffset: offset - limit

					data: result

				res.previousOffset = null if o.previousOffset < 0
				res.nextOffset = null if o.nextOffset > o.totalCount

				return done null, res


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



### ------- Export ------- ###

module.exports = (connString, options) ->
	return new Db connString, options