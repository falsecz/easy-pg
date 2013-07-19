debug	= require("debug") "easy-pg"
pg		= require "pg"
{Db}	= require "./lib/db"


### ------- Export ------- ###

module.exports = (conn) ->
	#debug "pg.native used: ", (not pg.native?)
	return new Db conn, pg

module.exports.__defineGetter__ "native", ->
	pg = require("pg").native
	return module.exports
