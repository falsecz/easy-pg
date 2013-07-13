debug = require("debug") "easy-pg"
url = require "url"
pg = require "pg" # . native TODO

{EventEmitter} = require "events"
{QueryObject} = require "#{__dirname}/queryObject.coffee"
{TransactionStack} = require "#{__dirname}/transactionStack.coffee"

###
 Deferred Postgresql Client Class

 Contains queue of queries processed only in the case of established
 connection. Connection is established only after the first query is
 inserted into the client's queue, if options.lazy is set to "true"
 or not set. It is able to keep itself connected even if some not fatal
 errors occure and continue in query queue processing. It is able to
 keep interrupted transaction on track as well.
###
class Db extends EventEmitter

	# connection parameters required by this client
	requiredConnParams = ["user",
						  #"pswd",
						  "host",
						  "port",
						  "db" ]


	###
	 Constructor of deferred postgresql client
	 opts.lazy = false makes db connection star immediately
	 @requires "conn" - connection string or object { user, pswd, host, port, db, opts{} }
	###
	constructor: (conn) ->
		@queue = [] #create client's queue for queries
		@transaction = new TransactionStack()

		# parse connection string if needed
		conn = @parseConn conn if typeof conn is "string"

		return @handleError "wrong connection parameter - not string, not object" if typeof conn isnt "object"

		# check connection parameters
		for param in requiredConnParams
			unless (param of conn and conn["#{param}"]?)
				return @handleError "#{param} missing in connection parameters"

		#create connection string for pg
		cStr  = "pg://#{conn.user}"
		cStr += ":#{conn.pswd}" if conn.pswd?
		cStr += "@#{conn.host}:#{conn.port}/#{conn.db}"

		#append options from opts
		if Object.keys(conn.opts).length #keyCount
			cStr += "?"
			cStr += "#{key}=#{val}&" for key, val of conn.opts
			cStr = cStr.substring 0, cStr.length-1 #remove last "&"
		else delete conn.opts if conn.opts?

		@connectionString = cStr #set client's connection string
		@state = "offline"       #set client's connection state

		# there's nothing more to do if options does not exist
		return unless conn.opts?

		@tryToConnect() if conn.opts.lazy is "no"


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
				return @handleError err #new Error "connection failed ..."
			else
				client.on "error", (err) => #try to connect again immediately
					return @handleError err #new Error "connection lost..."
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
	 @requires "code" -it is string with pg err code
	 @returns true if err is not fatal
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
	 To indicate beginning of transaction
	###
	startTransaction: () =>
		@transaction.flush()
		@inTransaction = yes


	###
	 Resets query queue to enable us to start whole
	 transaction from the beginning
	###
	restartTransaction: () =>
		@queue = @transaction.queue.concat @queue
		@transaction.flush()
		

	###
	 To indicate end of transaction
	###
	stopTransaction: () =>

		@inTransaction = no


	###
	 Pushes given input into queue for later dispatching
	 @requires "type", "query"
	###
	queuePush: (type, query, values, done) =>
		#for the case of calling (type, query, done)
		if typeof values is "function"
			done = values
			values = null

		#create object with special callback to remove query from queue when it is processed
		qObj = new QueryObject type, query, values, (err, result) =>
			@kill() if @wantToEnd

			#switch transaction state with successful begin
			@startTransaction() if query is "BEGIN" and @transaction.isEmpty() and not err?

			#remove first querry from queue (it is processed now)
			#if it is OK or error is on our side
			unless acceptable err?.code
				@queuePull(true)
				done? err, result

			else #try the failed query with acceptable code again
				 #unfortunately, it is necessary to reconnect
				@restartTransaction() if @inTransaction
				@state = "offline"
				@queuePull() #process next query in the queue

		@queue.push qObj #register another query
		@queuePull() if @queue.length is 1 #process first query in the queue


	###
	 Dispatches first query in the queue if its not empty
	 Tries to connect to db if the client state is "offline"
	###
	queuePull: (shiftFirst = false) =>
		removed = @queue.shift() if shiftFirst #shift solved here to make this Pull ALMOST ATOMIC
		@transaction.push removed if @inTransaction and removed?
		@stopTransaction() if @inTransaction and @transaction.isEmpty() #transaction done

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
	 @requires "query"
	###
	query: (query, values, done) =>

		@queuePush "QueryRaw", query, values, done


	###
	 Returns null or only the first row of the original DB result
	 @requires "query"
	###
	queryOne: (query, values, done) =>

		@queuePush "QueryOne", query, values, done


	###
	 Returns null or array of rows of the original DB result
	 @requires "query"
	###
	queryAll: (query, values, done) =>

		@queuePush "QueryAll", query, values, done


	###
	 Inserts "data" into specified "table"
	 @requires "table", "data"
	###
	insert: (table, data, done) =>
		parsed = @parseData data # parse data info arrays

		query = "INSERT INTO #{table} (#{parsed.keys.join ', '}) VALUES (#{parsed.valueIDs.join ', '}) RETURNING *"
		@queryOne query, parsed.values, done


	###
	 Updates specified "table" using given "data"
	 @requires "table", "data", "where"
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
	 @requires "table", "data", "where", "whereData"
	 @note requires Postgresql version 8.4, 9.0 or higher
	 because of "WITH" query statement
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
	 @requires "offset", "limit", "cols", "query"
	###
	paginate: (offset, limit, cols, query, values, done) =>
		if typeof values is "function"
			done = values
			values = null

		offset = parseInt offset
		limit = parseInt limit

		# separate query and its ORDER BY
		index = query.lastIndexOf "ORDER BY"
		index = query.lastIndexOf "order by" if index < 0
		orderBy = "ORDER BY #{cols}"

		if index > 0
			orderBy = query.substring index
			query = query.substring 0, index

		# queries we need to perform pagination
		#
		# this would be much safer using transaction
		# BEGIN  queryPart1  queryPart2  COMMIT
		# but it is about 20% slower
		queryPart1 = "SELECT COUNT(*) FROM (#{query}) AS countResult"
		queryPart2 = """
					 SELECT #{cols} FROM (#{query}) AS queryResult
					 #{orderBy}
					 OFFSET #{offset} LIMIT #{limit}
					 """

		clbckP1count = -1 #indicates err state if not changed
		callbackPart1 = (err, res) =>
			return done err if err?
			clbckP1count = parseInt res.count

		callbackPart2 = (err, res) =>
			return if clbckP1count is -1
			return done err if err?

			result =
				totalCount: clbckP1count
				previousOffset: offset - limit
				currentOffset:  offset
				nextOffset:     offset + limit
				data: res

			result.previousOffset = null if result.previousOffset < 0
			result.nextOffset = null if result.nextOffset > result.totalCount

			return done null, result

		@queryOne queryPart1, values, callbackPart1
		@queryAll queryPart2, values, callbackPart2


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
	 Sets savepoint for current transaction
	 @requires "pointName"
	###
	savepoint: (pointName, done) =>

		@query "SAVEPOINT #{pointName}", done


	###
	 Aborts current transaction, may use pointName
	 to abort the transaction just partialy
	###
	rollback: (pointName, done) =>
		if typeof pointName is "function"
			done = pointName
			pointName = null

		if pointName? then @query "ROLLBACK TO SAVEPOINT #{pointName}", done else @query "ROLLBACK", done


	###
	 Parses connection object from connection string
	 @requires "str" with connection string
	 @returns object with connection information
	###
	parseConn: (str) ->
		# url parse expects spaces encoded as %20
		result = url.parse (encodeURI str), true
		auth = result.auth?.split ":"

		connObj =
			user : auth[0] if auth?[0]?.length > 0
			pswd : auth[1] if auth?[1]?.length > 0
			host : result.hostname if result.hostname?.length > 0
			port : result.port if result.port?.length > 0
			db   : result.pathname.slice(1) if result.pathname?.length > 1
			opts : result.query

		return connObj


	###
	 Parses given data object to get format of this data
	 useful for client's query functions
	 @requires "data" object
	 @returns { "keys", "values", "valueIDs", "sets" }
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


	###
	 Handles errors
	 @requires "err", it has to be string or instance of Error
	###
	handleError : (err) =>
		err = new Error err if typeof err is "string"

		if (acceptable err?.code) #if it is not absolute failure, reconnect
			if (@state isnt "connecting") #if there's not another restart in progress
				@restartTransaction() if @inTransaction
				@tryToConnect()
		else # report error
			if @listeners("error").length then @emit "error", err else throw err


	###
	 Immediately stops the client, queries in the queue will be lost
	 The client can be reconnected, by inserting another query
	###
	kill: () =>
		clearTimeout @reconnectTimer if @reconnectTimer? # to prevent reconnecting after kill

		if @client?
			@queue.length = 0 # clear, but keeps all references to this queue
			@transaction.flush()
			@client.end()
			@emit "end" if @state isnt "online"
		
		clearTimeout @reconnectTimer if @reconnectTimer? # once more, just for sure

		@state = "offline"
		@inTransaction = no
		@wantToEnd = no


	###
	 Stops the client right after the last query callback, thus it
	 should not emit any query fail error
	###
	end: () =>
		unless @wantToEnd
			@wantToEnd = yes
			@kill() if @state isnt "online" or @queue.length is 0 #nothing to do

### ------- Export ------- ###

module.exports = (connString, options) ->
	return new Db connString, options
	