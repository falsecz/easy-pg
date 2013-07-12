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
	constructor: (type, query, values, done) ->
		#(type, query, done)
		if typeof values is "function"
			done = values
			values = null

		#set default call if query call is unknown
		type = "QueryAll" unless queryCall[type]?

		@query = query
		@values = values
		@done = done
		@type = type


	###
	 Forces this query to be sent to the pgdb
	 @requires "client" pgdb connection client
	###
	callBy: (client) =>

		queryCall[@type] client, @query, @values, @done


	###
	 Prints this query out in a readable form
	 @returns string representation of this query
	###
	toString: () =>

		"#{@type}: \"#{@query}\" + [#{@values}]"

	###
	 Different types of query call
	###
	queryCall= {
		#return response in the original form
		"QueryRaw" : (client, query, values, done) ->
			client.query query, values, (err, result) ->
				return done err, result

		#return only rows of the response
		"QueryAll" : (client, query, values, done) ->
			client.query query, values, (err, result) ->
				result = result?.rows
				result?= null
				return done err, result  #if done is function and result exists

		#return only the first row of the response
		"QueryOne" : (client, query, values, done) ->
			client.query query, values, (err, result) ->
				result = result?.rows?[0]
				result ?= null
				return done err, result
	}


### ------- Export ------- ###

module.exports.QueryObject = QueryObject