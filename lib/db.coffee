debug	= require("debug") "easy-pg-db"
url		= require "url"

{EventEmitter}		= require "events"
{QueryObject}		= require "./query-object"
{TransactionStack}	= require "./transaction-stack"

setImmediate = setImmediate ? process.nextTick 

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
	requiredConnParams = [
		"protocol"
		#"user"		not required
		#"password" not required
		"host"
		#"port"		not required
		"db"
	]
	
	#handling for individual conn.options
	optsHandler =
		#conn.options.lazy
		lazy: (client, val) =>
			client._tryToConnect() if val is "no" or val is "false"
		#conn.options.datestyle
		dateStyle: (client, val) =>
			client._optsPush "QueryRaw", "SET DATESTYLE = #{val}", (err, res) =>
				return client._handleError err if err?
		#conn.options.searchPath
		searchPath: (client, val) =>
			client._optsPush "QueryRaw", "SET SEARCH_PATH = #{val}", (err, res) =>
				return client._handleError err if err?
		#conn.options.serverVersion
		serverVersion: (client, val) =>
			client._optsPush "QueryRaw", "SELECT VERSION()", (err, res) =>
				return client._handleError err if err?
				client.serverVersion = res.rows[0].version.split(" ")[1].split(".")


	###
	Constructor of deferred postgresql client
	opts.lazy = false makes db connection star immediately
	@requires 	"conn" - connection string or object { user,
				password, host, port, db, options{} }
				"pg" - pure JavaScript or native libpq binding
	###
	constructor: (conn, pg) ->
		@pg = pg
		@queue = []		#queue for queries
		@optsQueue = [] #queue with options queries processed just on connection
		@transaction = new TransactionStack() #stack for transactions

		# parse connection string if needed
		conn = @_parseConn conn if typeof conn is "string"

		return @_handleError "wrong connection parameter - not string, not object" if typeof conn isnt "object"

		# check connection parameters
		for param in requiredConnParams
			unless (param of conn and conn["#{param}"]?)
				@_handleError "#{param} missing in connection parameters"
				return

		#create connection string for pg
		pswd = if conn.password? then ":#{conn.password}" else ""
		user = if conn.user? then "#{conn.user}#{pswd}@" else ""
		port = if conn.port? then ":#{conn.port}" else ""
		cStr  = "#{conn.protocol}//#{user}#{conn.host}#{port}/#{conn.db}"

		#append options from opts
		if Object.keys(conn.options).length #key count
			cStr += "?"
			cStr += "#{key}=#{val}&" for key, val of conn.options
			cStr = cStr.substring 0, cStr.length-1 #remove last "&"

		@connectionString = cStr #set client's connection string
		@state = "offline"       #set client's connection state

		# there's nothing more to do if options does not exist
		conn.options.serverVersion = null unless conn.options.serverVersion?

		# set all options except lazy because lazy may
		# force connection before all options are set
		for option, value of conn.options
			optsHandler[option](@, value) if optsHandler[option]? and option isnt "lazy"

		optsHandler["lazy"](@, conn.options.lazy) if optsHandler.lazy?


	###
	Returns full DB result with data in the ".rows" entry
	@requires "query"
	###
	query: (query, values, done) =>

		@_queuePush "QueryRaw", query, values, done


	###
	Returns null or only the first row of the original DB result
	@requires "query"
	###
	queryOne: (query, values, done) =>

		@_queuePush "QueryOne", query, values, done


	###
	Returns null or array of rows of the original DB result
	@requires "query"
	###
	queryAll: (query, values, done) =>

		@_queuePush "QueryAll", query, values, done


	###
	Returns full DB result with data in the ".rows" entry
	Queries in arrays are processed sequentialy in the core
	@requires "queries"
	###
	_querySequence: (queries, values, dones) =>
		#for the case of calling (type, query, done)
		@_queuePush "QuerySequence", queries, values, dones


	###
	Inserts "data" into specified "table"
	@requires	"table", "data" -object or array of objects
	@returns	array of inserted rows
	###
	insert: (table, data, done) =>
		parsed = @_parseData data # parse data into arrays

		if Array.isArray parsed
			keys = parsed[0].keys.join ", "
			values = []
			values.push obj.values... for obj in parsed
			valIds = []
			valIds.push "(#{obj.valueIDs.join ', '})"for obj in parsed
			valueIDs = valIds.join ", "
		else
			keys = parsed.keys.join ", "
			values = parsed.values
			valueIDs = " (#{parsed.valueIDs.join ', '})"

		query = "INSERT INTO #{table} (#{keys}) VALUES #{valueIDs} RETURNING *"
		
		return @queryAll query, values, done


	###
	Deletes data from specified "table"
	@requires	"table"
	@returns	array of deleted rows
	###
	delete: (table, where, values, done) =>
		if typeof where is "function"
			done = where
			query = "DELETE FROM #{table} RETURNING *"
		else
			query = "DELETE FROM #{table} WHERE #{where} RETURNING *"
		@queryAll query, values, done


	###
	Updates specified "table" using given "data"
	@requires	"table", "data", "where"
	@returns	array of updated rows
	###
	update: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		parsed = @_parseData data # parse "data" into arrays

		# parse data from "whereData" and match it in "where"
		i = parsed.values.length
		where = where.replace /\$(\d+)/g, (match, id) ->
			"$" + (i + parseInt(id))

		for val in whereData
			parsed.values.push val
		
		query = "UPDATE #{table} SET #{parsed.sets.join ', '} WHERE #{where} RETURNING *"
		@queryAll query, parsed.values, done


	###
	Updates (inserts) data in the specified "table"
	@requires	"table", "data", "where"
	@returns	object with .operation and .rows[]
	@note		requires Postgresql version 9.1 or higher which supports
				"WITH" query statement in combination with UPDATE or INSERT
				slower version of upsert will be used for older versions
	###
	upsert: (table, data, where, whereData=[], done) =>
		#version 9.1 and higher is needed to use one-query upsert
		if @serverVersion? and @serverVersion[0] >= 9 and @serverVersion[1] >= 1
			@upsertNew table, data, where, whereData, done
		else
			@upsertOld table, data, where, whereData, done

	#slower, using 2 queries in transaction, but works with older server versions
	upsertOld: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		parsed = @_parseData data # parse "data" into arrays

		# parse data from "whereData" and match it in "where"
		i = parsed.values.length
		where = where.replace /\$(\d+)/g, (match, id) =>
			"$" + (i + parseInt(id))

		#for val in whereData
		#	parsed.values.push val
		upValues = parsed.values.concat whereData

		queryPart1 = """
			UPDATE #{table}
			SET #{parsed.sets.join ', '}
			WHERE #{where}
			RETURNING *
			"""
		queryPart2 = "IF" #run next query if true, skip it if false, skipped
						#query will not be processed but its done will be
						#called with (null, null) passed in
		queryPart3 = """
			INSERT INTO #{table} (#{parsed.keys.join ', '})
			SELECT #{parsed.valueIDs.join ', '}
			RETURNING *
			"""

		result = null
		callbackPart1 = (err, res) =>
			return done? err if err?
			result = res if res.rows.length > 0

		#branching function for "IF", has to return only
		#true or false
		branchingFunc = (res) =>
			if res.rows.length > 0 then return false else return true

		callbackPart3 = (err, res) =>
			return done? err if err?

			if (result? and res?) or ((not result?) and (not res?))
				return done? new Error("upsert failure: one of insert or update should be used")

			result = res if res? # this query was skipped if res == null

			result =
				operation: result.command.toUpperCase()
				rows: result.rows

			return done? null, result

		allQueries =	[queryPart1,	queryPart2,		queryPart3]
		allValues =		[upValues,		null,			parsed.values]
		allCallbacks =	[callbackPart1,	branchingFunc,	callbackPart3]

		@_querySequence allQueries, allValues, allCallbacks

	#one-query upsert -fast and safe, but server version > 9.1 needed !
	upsertNew: (table, data, where, whereData=[], done) =>
		if typeof whereData is "function"
			done = whereData
			whereData = []

		parsed = @_parseData data # parse "data" into arrays

		# parse data from "whereData" and match it in "where"
		i = parsed.values.length
		where = where.replace /\$(\d+)/g, (match, id) ->
			"$" + (i + parseInt(id))

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
					WHERE NOT EXISTS (SELECT * FROM try_update)
					RETURNING *
				)

			SELECT 'update' AS operation, * FROM try_update
			UNION
			SELECT 'insert' AS operation, * FROM try_create
			"""

		@queryAll upsQuery, parsed.values, (err, res) =>
			return done? err if err?
			op = res[0].operation #store used operation

			#remove column with operation
			delete row.operation for row in res

			result =
				operation: op.toUpperCase()
				rows: res

			return done? null, result


	###
	Returns paginated "query" result containing max "limit" rows
	@requires	"offset", "limit", "cols", "query"
	@returns	object with total count of rows, offsets and one-page rows
	###
	paginate: (offset, limit, cols, query, values, done) =>
		if typeof values is "function"
			done = values
			values = null

		offset = parseInt offset
		limit = parseInt limit

		# separate query and its ORDER BY
		index = query.toLowerCase().lastIndexOf "order by"
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
			return done? err if err?
			clbckP1count = parseInt res.rows[0].count

		callbackPart2 = (err, res) =>
			return done? err if err?
			return new Error "pagination failed, count = -1" if clbckP1count is -1

			result =
				totalCount:		clbckP1count
				previousOffset:	offset - limit
				currentOffset:	offset
				nextOffset:		offset + limit
				data:			res.rows

			result.previousOffset = null if result.previousOffset < 0
			result.nextOffset = null if result.nextOffset > result.totalCount

			return done? null, result

		allQueries =	[queryPart1,	queryPart2]
		allValues =		[values,		values]
		allCallbacks =	[callbackPart1,	callbackPart2]

		@_querySequence allQueries, allValues, allCallbacks


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

		if pointName?
			@query "ROLLBACK TO SAVEPOINT #{pointName}", done
		else @query "ROLLBACK", done


	###
	Stops the client right after the last query callback, thus it
	should not emit any query fail error
	###
	end: () =>
		return if @wantToEnd
		@wantToEnd = yes
		@_kill() if @state isnt "online" or @queue.length is 0 #nothing to do


	###
	Immediately stops the client, queries in the queue will be lost
	The client can be reconnected, by inserting another query
	###
	_kill: () =>
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
	Tries to connect to DB. If connection drops or some
	error occures, tries it later
	###
	_tryToConnect: () =>
		@state = "connecting"
		@client = new @pg.Client @connectionString
		@client.connect (err) =>
			if err #register request for later connection
				@reconnectTimer = setTimeout @_tryToConnect, 2000
				return @_handleError err #new Error "connection failed ..."
			
			@client.on "error", (err) => #try to connect again immediately
				return @_handleError err #new Error "connection lost..."
				@_tryToConnect()
			@client.on "end", () =>
				@emit "end"

			@state = "online"
			# set all opts and callback "ready" and _queuePull
			@queue = @optsQueue.concat @queue
			@emit "ready"
			@_queuePull()   #process first query in the queue immediately


	###
	Returns true if the given postgresql error code
	is _acceptable = there is no reason to remove query
	with this code from our queue or to throw error
	@requires "code" -it is string with pg err code
	@returns  true if err is not fatal
	###
	_acceptable: (code) ->
		return no unless code?
		errClass = code.slice(0, 2) #first two signs describe error class
		errClass in [
			"00"	# Successful Completion
			"08"	# Connection Exception
			"57"	# Operator Intervention
		]


	###
	To indicate beginning of transaction
	###
	_transStart: () =>
		@transaction.flush()
		@inTransaction = yes


	###
	Resets query queue to enable us to start whole
	transaction from the beginning
	###
	_transRestart: () =>
		@queue = @transaction.queue.concat @queue
		@transaction.flush()
		

	###
	To indicate end of transaction
	###
	_transStop: () =>

		@inTransaction = no


	###
	Creates query object with client specific callback
	@requires "type", "query"
	@returns  query object with callback connecten to
	          client's queue of queries
	###
	_createQueryObject: (type, query, values, done) =>
		#for the case of calling (type, query, done)
		if typeof values is "function"
			done = values
			values = null

		#in the case of query sequence
		if Array.isArray query
			for i in [0...query.length]
				if typeof values[i] is "function"
					done[i] = values[i]
					values[i] = null
			dones = done
			done = dones[dones.length-1]

		callback = (err, result) =>
			@_kill() if @wantToEnd

			#switch transaction state with successful begin
			unless Array.isArray query
				keyWord = query.toUpperCase().trim().split(" ", 1)[0]
				@_transStart() if keyWord is "BEGIN" and @transaction.isEmpty() and not err?

			#remove first querry from queue (it is processed now)
			#if it is OK or error is on our side
			unless @_acceptable err?.code
				@_queuePull(true)
				done? err, result

			else #try the failed query with _acceptable code again
				 #unfortunately, it is necessary to reconnect
				@_transRestart() if @inTransaction
				@state = "offline"
				@_queuePull() #process next query in the queue

		if Array.isArray query
			dones[dones.length-1] = callback
			callback = dones

		#create object with special callback to remove query
		#from queue when it is processed, return it
		return new QueryObject type, query, values, callback


	###
	Pushes given input into queue for later dispatching
	@requires "type", "query"
	###
	_queuePush: (type, query, values, done) =>
		qObj = @_createQueryObject type, query, values, done
		
		@queue.push qObj #register another query
		@_queuePull() if @queue.length is 1 #process first query in the queue


	###
	Pushes given input into options queue for later dispatching
	@requires "type", "query"
	###
	_optsPush: (type, query, values, done) =>
		qObj = @_createQueryObject type, query, values, done
		@optsQueue.push qObj #register another query


	###
	Dispatches first query in the queue if its not empty
	Tries to connect to db if the client state is "offline"
	###
	_queuePull: (shiftFirst = false) =>
		removed = @queue.shift() if shiftFirst #shift solved here to make this Pull ALMOST ATOMIC
		@transaction.push removed if @inTransaction and removed?
		@_transStop() if @inTransaction and @transaction.isEmpty() #transaction done

		if @queue.length > 0
			@queue[0].callBy @client if @state is "online"
			@_tryToConnect() if @state is "offline"


	###
	Just for debug, prints queue content
	###
	_printQueue: () =>
		console.log "---------------------------"
		console.log q.toString() for q in @queue
		console.log "---------------------------"


	###
	Parses connection object from connection string
	@requires "str" with connection string
	@returns  object with connection information
	###
	_parseConn: (str) ->
		# url parse expects spaces encoded as %20
		result = url.parse (encodeURI str), true
		auth = result.auth?.split ":"

		connObj =
			protocol:	result.protocol
			user:		(auth[0] if auth?[0]?.length > 0)
			password:	(auth[1] if auth?[1]?.length > 0)
			host:		result.hostname if result.hostname?.length > 0
			port:		result.port if result.port?.length > 0
			db:			result.pathname.slice(1) if result.pathname?.length > 1
			options:	result.query

		return connObj


	###
	Parses given data object to get format of this data
	useful for client's query functions
	@requires	"data" object
	@returns	{ "keys", "values", "valueIDs", "sets" } or array
				of objects like this one
	###
	_parseData: (data) ->
		unless Array.isArray data # inserting multiple rows
			data = [data]

		result = []
		i = 1

		for obj in data
			keys = []		# array of keys
			values = []		# array of values
			valueIDs = []	# array of "$id"
			sets = []		# array of "key = $id"

			for key, val of obj
				keys.push key
				values.push val
				valueIDs.push "$#{i}"
				sets.push "#{key} = $#{i}"
				i++

			result.push { keys, values, valueIDs, sets }

		return result[0] if result.length is 1
		return result


	###
	Handles errors
	@requires "err", it has to be string or instance of Error
	###
	_handleError : (err) =>
		err = new Error err if typeof err is "string"

		if (@_acceptable err?.code) #if it is not absolute failure, reconnect
			if (@state isnt "connecting") #if there's not another restart in progress
				@_transRestart() if @inTransaction
				@_tryToConnect()
		else # report error
			if @listeners("error").length then setImmediate =>
				@emit "error", err
			else throw err


### ------- Export ------- ###

module.exports.Db = Db
