debug = require('debug') 'easy-pg-qo'

class QueryObject

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

	callBy: (client) =>
		queryCall[@type] client, @query, @values, @done

	toString: () =>
		"#{@type}: \"#{@query}\" + [#{@values}]"

	queryCall= {
		"QueryRaw" : (client, query, values, done) ->
			client.query query, values, (err, result) ->
				return done err, result

		"QueryOne" : (client, query, values, done) ->
			debug "QueryOne: ", typeof client

			client.query query, values, (err, result) ->
				debug "QueryOne - clientCallback"
				result = result?.rows?[0]
				result ?= null
				return done err, result

		"QueryAll" : (client, query, values, done) ->
			client.query query, values, (err, result) ->
				result = result?.rows
				result?= null
				return done err, result  #if done is function and result exists
	}


### ------- Export ------- ###

module.exports.QueryObject = QueryObject