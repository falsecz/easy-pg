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
			"savepoint"
			"rollback"
			"end"
			"on"
		]
		for f in fn
			assert.isFunction db[f], "must have #{f} function"

	it "throw error on incomplete connection string information", () ->
		assert.throws (-> pg incompleteConnectionStr, lazy: no ), Error

	it "throw error on incomplete connection object information", () ->
		assert.throws (-> pg incompleteConnection, lazy: no ), Error

	it "throw error on wrong type of connection parameter", () ->
		assert.throws (-> pg 90210, lazy: no ), Error

	it "emit error on couldn't connect", (done) ->
		db = pg wrongConnectionStr, lazy: no
		db.on "error", (err) ->
			db.end() # to stop calling ConnErr all over again
			return done()

	it "emit ready on successfull connection", (done) ->
		db = pg connectionStr, lazy: no
		db.on "ready", (err) ->
			db.end()
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

	it "throw error on incomplete connection string information", () ->
		assert.throws (-> pg incompleteConnectionStr ), Error

	it "throw error on incomplete connection object information", () ->
		assert.throws (-> pg incompleteConnection ), Error

	it "throw error on wrong type of connection parameter", () ->
		assert.throws (-> pg 90210 ), Error

	it "emit error on couldn't connect", (done) ->
		db = pg wrongConnectionStr
		db.on "error", (err) ->
			db.end() # to stop calling ConnErr all over again
			return done()

		setTimeout ( ->
			db.query "SELECT 1 WHERE 1 = 1"
			), 500

	it "emit ready on successfull connection", (done) ->
		db = pg connectionStr
		db.on "ready", (err) ->
			db.end()
			return done()

		setTimeout ( ->
			db.query "SELECT 1 WHERE 1 = 1"
			), 500

describe "Disconnection test", ->
	@timeout 10000 # 10sec

	# call kill to stop client working
	it "emit end on calling kill", (done) ->
		db = pg connectionStr, lazy: no
		db.on "ready", () ->
			setTimeout db.kill, 200

		db.on "end", () ->
			return done()

	it "emit end on calling end", (done) ->
		db = pg connectionStr, lazy: no
		db.on "ready", () ->
			setTimeout db.end, 200

		db.on "end", () ->
			return done()

	# insert query, kill clients work, insert another query to revive client
	it "end and revive by pushing new query afterwards", (done) ->
		db = pg connectionStr
		db.on "end", () ->
			return done() if itsTimeToEnd

		itsTimeToEnd = no

		db.query "SELECT 1 WHERE 1 = 1"
		db.query "SELECT 1 WHERE 1 = 1"
		db.query "SELECT 1 WHERE 1 = 1"#, (err, res) ->
		db.end()

		setTimeout ( ->
			db.query "SELECT 1 WHERE 1 = 1"
			db.query "SELECT 1 WHERE 1 = 1"
			db.query "SELECT 1 WHERE 1 = 1", (err, res) ->
			itsTimeToEnd = yes
			db.end()
			), 200
