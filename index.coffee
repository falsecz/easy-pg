{Db} = require "./lib/db"


### ------- Export ------- ###

module.exports = (conn) ->
	return new Db conn

module.exports.__defineGetter__ "native", ->
	pg = require("pg").native
	return module.exports
