pg = if process.env.NATIVE then require("../").native else require "../"

connectionStr = "pg://postgres@127.0.0.1:5432/myapp_test?lazy=no"

QUERY_DROP = "DROP TABLE IF EXISTS numbers;"
QUERY_CREATE = "CREATE TABLE IF NOT EXISTS numbers (_id bigserial primary key, number int NOT NULL);"

describe "Querying functions", ->
	@timeout 10000 # 10sec
	db = pg connectionStr
	db.on 'error', (err) ->
			console.log err

	beforeEach ->
		#clear db-table numbers
		db.query QUERY_DROP   #ignore error
		db.query QUERY_CREATE #ignore error

	describe "insert", ->
		it "returns result on right query", (done) ->
			db.insert "numbers", number: 99, (err, res) ->
				return done err if err?
				return done() if (res? and res.number is 99)

		it "returns error on wrong query", (done) ->
			db.insert "table", value: 0, (err, res) ->
				return done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			INSERT_COUNT = 100

			for i in [0...INSERT_COUNT]
				db.insert "numbers", number: i #ignore error

			#get number of inserts
			db.queryOne "SELECT COUNT(*) FROM numbers;", (err, res) -> #ignore error
				return done err if err?
				return done() if (parseInt res.count, 10) is INSERT_COUNT

	describe "update", ->
		it "returns result on right query", (done) ->
			db.insert "numbers", number: 99
			db.update "numbers", number: 0, "number = 99", (err, res) ->
				return done err if err?
				return done() if (res? and res.number is 0)

		it "returns error on wrong query", (done) ->
			db.update "table", value: 0, "value = 99", (err, res) ->
				return done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			UPDATE_COUNT = INSERT_COUNT = 100

			for i in [0...INSERT_COUNT]
				db.insert "numbers", number: i #ignore error

			for j in [0...UPDATE_COUNT]
				db.update "numbers", number: 0, "number = #{j}" #ignore error

			db.queryOne "SELECT SUM(number) FROM numbers;", (err, res) -> #ignore error
				return done err if err?
				return done() if (parseInt res.sum, 10) is 0

	describe "upsert", ->
		# insert, and uppsert causing one insert followed by two updates expected
		it "returns result on right query", (done) ->
			db.insert "numbers", number: 0 #ignore error
			db.upsert "numbers", number: 1, "number = $1", [2], (err, res) ->
				return done err if err?
				unless res[0]?.operation is "insert" and res.length is 1
					return done new Error "upsert-insert failed"
				db.upsert "numbers", number: 0, "number = $1 OR number = $2", [0, 1], (err, res) ->
					return done err if err?
					if res[0]?.operation is "update" and res.length is 2 then return done()
					else return done new Error "upsert-update failed"

		it "returns error on wrong query", (done) ->
			db.upsert "table", value: 0, "value = 99", (err, res) ->
				return done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			UPSERT_COUNT = 50

			for i in [0...UPSERT_COUNT]
				db.upsert "numbers", number: i, "number = #{i}" #ignore error

			for j in [0...UPSERT_COUNT]
				db.upsert "numbers", number: 0, "number = $1", [j] #ignore error

			db.queryOne "SELECT COUNT(number) FROM numbers;", (err, res) -> #ignore error
				return done err if err?
				return done() if (parseInt res.count, 10) is UPSERT_COUNT

	describe "paginate", ->
		it "returns result on right query", (done) ->
			INSERT_COUNT = 10

			for i in [0...INSERT_COUNT]
				db.insert "numbers", number: i #ignore error

			db.paginate 0, 10, "_id, number", "SELECT * FROM numbers WHERE _id > $1 ORDER BY _id", [9], (err, res) ->
				return done err if err?
				return done() if res.totalCount is 1

		it "returns error on wrong query", (done) ->
			db.paginate 0, 10, "_id, number", "SELECT * FROM table", (err, res) ->
				return done() if err?

		it "successful sequence of 100 fast queries", (done) ->
			PAGE_COUNT = INSERT_COUNT = 100

			for i in [0...INSERT_COUNT]
				db.insert "numbers", number: i #ignore error

			for i in [0...PAGE_COUNT]
				db.paginate i, 10, "_id, number", "SELECT * FROM numbers", (err, res) ->
					if err?
						db.end()
						return done err
					# if last page
					if res.totalCount is PAGE_COUNT and res.nextOffset is null
						if res.currentOffset is PAGE_COUNT - 1
							db.end()
							return done()

