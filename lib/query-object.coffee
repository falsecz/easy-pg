debug = require('debug') 'easy-pg-qo'


###
Query Object Class

Contains all necessary info about requested pg query.
It can be sent to pgdb by calling "callBy"
###
class QueryObject

	###
	Query Object constructor, if query type is unknown
	it's set to "QueryAll"
	@requires "type", "query", "done" -it's callback
	###
	constructor: (@type, @query, @values, @done) ->
		#@type = type, @query = query,...

		#(type, query, done)
		if typeof @values is "function"
			@done = @values
			@values = null

		#set default call if query call is unknown
		@type = "QueryAll" unless @_queryCall[@type]?


	###
	Forces this query to be sent to the pgdb
	@requires "client" pgdb connection client
	###
	callBy: (client) =>
		debug "calling", @toString()
		@_queryCall[@type] client, @query, @values, @done


	###
	Prints this query out in a readable form
	@returns  string representation of this query
	###
	toString: () =>
		values =  if @values? then " + [#{@values}]" else ""
		"#{@type}: \"#{@query}\"#{values}"

	###
	Different types of query call
	###
	_queryCall:
		#return response in the original form
		"QueryRaw" : (client, query, values, done) =>
			client.query query, values, (err, result) =>
				return done err, result

		#return only rows of the response
		"QueryAll" : (client, query, values, done) =>
			client.query query, values, (err, result) =>
				result = result?.rows
				result?= null
				return done err, result

		#return only the first row of the response
		"QueryOne" : (client, query, values, done) =>
			client.query query, values, (err, result) =>
				result = result?.rows?[0]
				result?= null
				return done err, result

		#return response in the original form
		"QuerySequence" : (client, queries, values, dones) =>
			client.query "BEGIN", (err, res) =>
				return _handleError client, dones, err if err?
				_callQuery client, queries, values, dones, 0, (err, result) =>
					return _handleError client, dones, err if err?
					client.query "COMMIT", (err, res) =>
						dones[dones.length-1] err, result
	
	_handleError= (client, dones, error) =>
		client.query "ROLLBACK", (err, res) =>
			dones[dones.length-1] error

	_callQuery= (client, queries, values, dones, index, callback) =>
		client.query queries[index], values[index], (err, res) =>
			return callback err if err?
			dones[index]? err, res
			index++

			#branching !
			if queries[index].toUpperCase() is "IF"
				index++
				if dones[index-1](res) is false #branching func returning true/false only
					index++

			if index < queries.length-1
				_callQuery client, queries, values, dones, index, callback
			else if index == queries.length-1 #last query
				client.query queries[index], values[index], callback
			else
				callback null, null #jump out to COMMIT



### ------- Export ------- ###

module.exports.QueryObject = QueryObject
