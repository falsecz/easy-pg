debug = require('debug') 'easy-pg'
url = require "url"
pg = require "pg" # . native TODO

{EventEmitter} = require "events"
{QueryObject} = require "#{__dirname}/queryObject.coffee"

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
		@queue = [] #create client's queue for queries

		# parse connection string if needed
		conn = @parseConn conn if typeof conn is "string"

		if typeof conn is "object"
			# check connection parameters
			for param in requiredConnParams
				unless (param of conn and conn["#{param}"]?)
					return @handleError(new Error "#{param} missing in connection parameters")

			#create connection string for pg
			connString = "pg://#{conn.user}:#{conn.pswd}@#{conn.host}:#{conn.port}/#{conn.db}"

		else #just use the given connection string
			return @handleError(new Error "wrong connection parameter - not string, not object")


		@connectionString = connString #set client's connection string
		@state = "offline"             #set client's connection state

		# theres nothing more to do if options does not exist
		return unless opts?

		@tryToConnect() if opts.lazy is no

		# ONLY IF SET IN OPTS
		# 	@query """ SET datestyle = "iso, mdy" """
		# 	@query """ set search_path public """


	###
	 Handles errors
	###
	handleError : (err) ->
		if (!@_events || !@_events.error || (typeIsArray(@_events.error) && !@_events.error.length))
			setImmediate =>
				@emit "error", err
		else
			throw err


	###
	 Immediately stops the client, queries in the queue will be lost
	 The client can be reconnected, by inserting another query
	###
	kill: () =>
		#@state = "killing"
		if @reconnectTimer? # to prevent reconnecting after kill
			clearTimeout @reconnectTimer
		if @client?
			@queue.length = 0 # clear, but keeps all references to this queue
			@client.end()
			@emit "end" if @state isnt "online"
		if @reconnectTimer? # once more, just for sure
			clearTimeout @reconnectTimer

		@state = "offline"


	###
	 Tries to connect to DB. If connection drops or some
	 error occures, tries it later
	###
	tryToConnect: () =>
		@state = "connecting"
		@client = new pg.Client @connectionString
		@client.connect (err, client) =>
			if err #register request for later connection
				@reconnectTimer = setTimeout @tryToConnect, 2000
				@emit "error", err.toString() #new Error "connection failed ..."
			else
				client.on "error", (err) => #try to connect again immediately
					@emit "error", err.toString() #new Error "connection lost..."
					@tryToConnect()
				client.on "end", () =>
					@emit "end"

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
		parsed = @parseData data # parse data info arrays

		query = "INSERT INTO #{table} (#{parsed.keys.join ', '}) VALUES (#{parsed.valueIDs.join ', '}) RETURNING *"
		@queryOne query, parsed.values, done


	###
	 Updates specified "table" using given "data"
	 requires "table", "data", "where", "done"
	###
	update: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		parsed = @parseData data # parse "data" info arrays

		# parse data from "whereData" and match it in "where"
		i = parsed.values.length
		where = where.replace /\$(\d+)/g, (match, id) ->
			"$" + (i - 1 + parseInt(id))

		for val in whereData
			parsed.values.push val

		query = "UPDATE #{table} SET #{parsed.sets.join ', '} WHERE #{where} RETURNING *"
		@queryOne query, parsed.values, done


	###
	 Updates (inserts) data in the specified "table"
	 requires "table", "data", "where", "whereData", "done"
	###
	upsert: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		parsed = @parseData data # parse "data" info arrays

		# parse data from "whereData" and match it in "where"
		i = parsed.values.length
		where = where.replace /\$(\d+)/g, (match, id) ->
			"$" + (i - 1 + parseInt(id))

		for val in whereData
			parsed.values.push val

		# ugly query, but the only way how to make UPSERT atomic
		# is make it work only id db
		upsQuery =  """
					WITH
					  try_update AS (
					    UPDATE #{table}
					    SET #{parsed.sets.join ', '}
					    WHERE #{where}
					    RETURNING *
					  ),
					  try_create AS (
					    INSERT INTO #{table} (#{parsed.keys.join ', '})
					    SELECT #{parsed.valueIDs.join ', '}
					    WHERE NOT EXISTS (SELECT 1 FROM try_update)
					    RETURNING *
					  )
					SELECT COALESCE((SELECT 1 FROM try_create), (SELECT 1 FROM try_update))
					"""

		@queryOne upsQuery, parsed.values, done


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


	###
	 Returns true if type is Array
	###
	typeIsArray = Array.isArray || ( value ) -> return {}.toString.call( value ) is '[object Array]'


	###
	 Parses connection object from connection string
	###
	parseConn: (str) ->
		# url parse expects spaces encoded as %20
		result = url.parse encodeURI str
		auth = result.auth?.split ":"
		connObj =
			user : auth[0] if auth?[0]?.length > 0
			pswd : auth[1] if auth?[1]?.length > 0
			host : result.hostname if result.hostname?.length > 0
			port : result.port if result.port?.length > 0
			db : result.pathname.slice(1) if result.pathname?.length > 1

		return connObj


	###
	 Parses given data object to get format of this data
	 useful for client's query functions
	 returned object contains: "keys", "values", "valueIDs", "sets"
	###
	parseData: (data) ->
		keys = []   # array of keys
		vals = []   # array of values
		valIds = [] # array of "$id"
		sets = []   # array of "key = $id"
		i = 1

		for key, val of data
			keys.push key
			vals.push val
			valIds.push "$#{i}"
			sets.push "#{key} = $#{i}"
			i++

		return { keys: keys, values: vals, valueIDs: valIds, sets: sets }	




### ------- Export ------- ###

module.exports = (connString, options) ->
	return new Db connString, options