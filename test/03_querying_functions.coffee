pg = require "../"

connectionStr = "pg://postgres:123456@localhost:5432/TestDB"

QUERY_DROP = "DROP TABLE IF EXISTS numbers;"
QUERY_CREATE = "CREATE TABLE IF NOT EXISTS numbers (_id bigserial primary key, number int NOT NULL);"

describe "Querying functions", ->
	this.timeout 10000 # 10sec

	beforeEach ->
		@db = pg connectionStr, lazy: no

		#clear db-table numbers
		@db.query QUERY_DROP   #ignore error
		@db.query QUERY_CREATE #ignore error
	
	describe "insert", ->
		it "returns result on right query", (done) ->
			@db.insert "numbers", number: 99, (err, res) =>
				done() if (res? and res.number is 99)

		it "returns error on wrong query", (done) ->
			@db.insert "table", value: 0, (err, res) =>
				done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			INSERT_COUNT = 100
			
			for i in [0...INSERT_COUNT]
				@db.insert "numbers", number: i #ignore error

			#get number of inserts
			@db.queryOne "SELECT COUNT(*) FROM numbers;", (err, res) -> #ignore error
				done() if (parseInt res.count, 10) is INSERT_COUNT

	describe "update", ->
		it "returns result on right query", (done) ->
			@db.insert "numbers", number: 99
			@db.update "numbers", number: 0, "number = 99", (err, res) =>
				done() if (res? and res.number is 0)

		it "returns error on wrong query", (done) ->
			@db.update "table", value: 0, "value = 99", (err, res) =>
				done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			UPDATE_COUNT = INSERT_COUNT = 100
			
			for i in [0...INSERT_COUNT]
				@db.insert "numbers", number: i #ignore error

			for j in [0...UPDATE_COUNT]
				@db.update "numbers", number: 0, "number = #{j}" #ignore error

			@db.queryOne "SELECT SUM(number) FROM numbers;", (err, res) -> #ignore error
				done() if (parseInt res.sum, 10) is 0

	describe "upsert", ->
		it "returns result on right query", (done) ->
			@db.upsert "numbers", number: 0, "number = 0", (err, res) =>
				@db.upsert "numbers", number: 0, "number = 0", (err, res) =>
					done() if (res? and res.coalesce is 1)
		
		it "returns error on wrong query", (done) ->
			@db.upsert "table", value: 0, "value = 99", (err, res) =>
				done() if err?
		
		it "successful sequence of 100 fast queries", (done) ->
			UPSERT_COUNT = 50

			for i in [0...UPSERT_COUNT]
				@db.upsert "numbers", number: i, "number = #{i}" #ignore error

			for j in [0...UPSERT_COUNT]
				@db.upsert "numbers", number: 0, "number = #{j}" #ignore error

			@db.queryOne "SELECT COUNT(number) FROM numbers;", (err, res) -> #ignore error
				done() if (parseInt res.count, 10) is UPSERT_COUNT


