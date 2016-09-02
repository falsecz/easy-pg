debug = require("debug") "easy-pg"
pg = require "pg"
{Db} = require "./db"


pg.on "error", (err) ->
	console.log "easy-pg: pg.on error:", err
### ------- Export ------- ###

module.exports = (conn) ->
	return new Db conn, pg
