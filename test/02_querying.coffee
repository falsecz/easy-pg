pg = require "../"

connectionStr = "pg://postgres:123456@localhost:5432/TestDB"

describe "Querying", ->
	@timeout 10000 # 10sec
	db = pg connectionStr

	beforeEach ->
		#@db = pg connectionStr, lazy: no

	describe "query", ->

		it "query callback called", (done) ->
			db.query "SELECT 1 WHERE 1 = 1", done

		it "query with data callback called", (done) ->
			db.query "SELECT 1 WHERE 1 = $1", [1], done

		it "query with long data callback called", (done) ->
			db.query """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], done

	describe "queryOne", ->
		it "callback called", (done) ->
			db.queryOne "SELECT 1 WHERE 1 = 1", done

		it "with data callback called", (done) ->
			db.queryOne 'SELECT 1 WHERE 1 = $1', [1], done

		it "with long data callback called", (done) ->
			db.queryOne """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], done

	describe "queryAll", ->
		it "callback called", (done) ->
			db.queryAll "SELECT 1 WHERE 1 = 1", done

		it "with data callback called", (done) ->
			db.queryAll 'SELECT 1 WHERE 1 = $1', [1], done

		it "with long data callback called", (done) ->
			db.queryAll """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], () ->
				db.end()
				done()

