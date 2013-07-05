pg = require "../"

connectionStr = "pg://postgres:123456@localhost:5432/TestDB"
connection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "TestDB"

incompleteConnectionStr = "pg://postgres:123456/TestDB"
incompleteConnection =
	user: "postgres"
	pswd: "123456"
	#host: "localhost"
	#port: "5432"
	db:   "TestDB"

wrongConnectionStr = "pg://postgres:123456@localhost:5432/unknown_database"
wrongConnection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "unknown_database"


describe "Immediate initialization", ->
	@timeout 10000 # 10sec

	it "create instance", () ->
		db = pg connectionStr, lazy: no
		db.on "ready", db.kill

		assert.isObject db, "db must be an object"
		fn = [
			"query"
			"queryAll"
			"queryOne"
			"insert"
			"update"
			"upsert"
			"paginate"
			"begin"
			"commit"
			"rollback"
			"on"
		]
		for f in fn
			assert.isFunction db[f], "must have #{f} function"

	it "emit error on incomplete connection string information", (done) ->
		db = pg incompleteConnectionStr, lazy: no
		db.on "error", (err) ->
			return done()

	it "emit error on incomplete connection object information", (done) ->
		db = pg incompleteConnection, lazy: no
		db.on "error", (err) ->
			return done()

	it "emit error on wrong type of connection parameter", (done) ->
		db = pg 90210, lazy: no
		db.on "error", (err) ->
			return done()

	it "emit error on couldn't connect", (done) ->
		db = pg wrongConnectionStr, lazy: no
		db.on "error", (err) ->
			db.kill() # to stop calling ConnErr all over again
			return done()

	it "emit ready on successfull connection", (done) ->
		db = pg connectionStr, lazy: no
		db.on "ready", (err) ->
			db.kill()
			return done()

describe "Deferred initialization", ->
	@timeout 10000 # 10sec

	it "create instance", () ->
		db = pg connectionStr, lazy: yes

		assert.isObject db, "db must be an object"
		fn = [
			"query"
			"queryAll"
			"queryOne"
			"insert"
			"update"
			"upsert"
			"paginate"
			"begin"
			"commit"
			"rollback"
			"on"
		]
		for f in fn
			assert.isFunction db[f], "must have #{f} function"

	it "emit error on incomplete connection string information", (done) ->
		db = pg incompleteConnectionStr
		db.on "error", (err) ->
			return done()

	it "emit error on incomplete connection object information", (done) ->
		db = pg incompleteConnection
		db.on "error", (err) ->
			return done()

	it "emit error on wrong type of connection parameter", (done) ->
		db = pg 90210
		db.on "error", (err) ->
			return done()

	it "emit error on couldn't connect", (done) ->
		db = pg wrongConnectionStr
		db.on "error", (err) ->
			db.kill() # to stop calling ConnErr all over again
			return done()

		setTimeout ( ->
			db.query "SELECT 1 WHERE 1 = 1"
			), 500

	it "emit ready on successfull connection", (done) ->
		db = pg connectionStr
		db.on "ready", (err) ->
			db.kill()
			return done()

		setTimeout ( ->
			db.query "SELECT 1 WHERE 1 = 1"
			), 500

describe "Kill test", ->
	@timeout 10000 # 10sec

	# call kill to stop client working
	it "emit end on calling kill", (done) ->
		db = pg connectionStr, lazy: no
		db.on "ready", () ->
			setTimeout db.kill, 200

		db.on "end", () ->
			return done()

	# insert query, kill clients work, insert another query to revive client
	it "kill and revive by pushing new query afterwards", (done) ->
		db = pg connectionStr
		db.on "end", () ->
			setTimeout ( ->
				db.query "SELECT 1 WHERE 1 = 1"
				db.query "SELECT 1 WHERE 1 = 1"
				db.query "SELECT 1 WHERE 1 = 1", (err, res) ->
					return done()
				), 200

		db.query "SELECT 1 WHERE 1 = 1"
		db.query "SELECT 1 WHERE 1 = 1"
		db.query "SELECT 1 WHERE 1 = 1", (err, res) ->
				setTimeout db.kill, 200
