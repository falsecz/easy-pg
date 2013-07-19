debug	= require("debug") "easy-pg"
pg		= require "pg"
{Db}	= require "./lib/db"


### ------- Export ------- ###

module.exports = (conn) ->
	#detect if pg is native, native doesn't have "native" getter
	debug "pg.native used: ", (not pg.hasOwnProperty "native")
	return new Db conn, pg

module.exports.__defineGetter__ "native", ->
	pg = require("pg").native
	return module.exports
