pg = if process.env.NATIVE then require("../").native else require "../"

connOpts = "?lazy=no&dateStyle=iso, mdy&searchPath=public&poolSize=1"
connectionStr = "pg://postgres@localhost/myapp_test" + connOpts


describe "Querying", ->
	@timeout 10000 # 10sec
	db = null

	beforeEach ->
		db = pg connectionStr if db is null

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
			""", [1, 1, 1, 1], (err, res) ->
				db.end()
				return done err, res

