NATIVE = 1
pg = if NATIVE then require("./index").native else pg = require "./index"


#connectionStr = "pg://postgres:123456@127.0.0.1:5432/myapp_test"
connectionStr = "pg://postgres@localhost/myapp_test"
connectionOpts = "?lazy=yes&datestyle=iso, mdy&searchPath=public&poolSize=1"

db = pg connectionStr+connectionOpts

db.on "error", (err) ->
	console.log "err: ",err

db.on "ready", () ->
	console.log "Deferred PG Client ready..."

db.on "end", (err) ->
	console.log "Client is over"


getAllNames = () ->
	db.queryAll 'SELECT * FROM names', (err, res) ->
		console.log "query fail ...", err if err
		console.log "getAllNames"
		console.log res

getFirstOfAllNames = () ->
	db.queryOne 'SELECT * FROM names', (err, res) ->
		console.log "query fail ...", err if err
		console.log "getFirstOfAllNames"
		console.log res

getName = (name) ->
	db.queryAll 'SELECT * FROM names WHERE firstname = $1', [name], (err, res) ->
		console.log "query fail ...", err if err
		console.log "getName(#{name})"
		console.log res

getNameRaw = (name) ->
	db.query 'SELECT * FROM names WHERE firstname = $1', [name], (err, res) ->
		console.log "query fail ...", err if err
		console.log "getNameRaw(#{name})"
		console.log res.rows

insertNum = (num) ->
	db.insert "numbers", number: num,(err, res) ->
		console.log "query fail ...", err if err
		console.log "inserted num #{num}"


insertName = (first, last) ->
	db.insert "names", {firstname: first, lastname: last},(err, res) ->
		console.log "query fail ...", err if err
		console.log "insertName(#{first}, #{last})"
		console.log res


console.log "db state: ", db.state


# test INSERT
INSERT_COUNT = 100
c = 1
pom = 0


fooStart = () ->
	#clear db-table numbers
	db.query 'DROP TABLE IF EXISTS numbers;', (err, res) ->
		console.log "DROP TABLE query fail ...", err if err

	db.query "CREATE TABLE IF NOT EXISTS numbers (_id bigserial primary key, number int NOT NULL);", (err, res) ->
		console.log "CREATE TABLE query fail ...", err if err

	db.begin () ->
		console.log "transaction begin"
	
	foo()

foo = () ->
	insertNum c
	if c is 20 then db.savepoint "x20savepoint", (err, res) ->
		console.log "savepoint set to first 20"
	if c is 50 then db.rollback "x20savepoint", () ->
		console.log "rolled back"
	return pom = setTimeout(foo, 20) if c++ < INSERT_COUNT

	#in the end
	db.commit () ->
		console.log "transaction commited"
		db.upsert "numbers", number : -1, "_id = $1 OR _id = $2", [70, 80], (err, res) ->
			console.log err, res
			db.delete "numbers", "_id = $1 OR _id = $2", [70, 80], (err, res)->
				console.log err
				console.log res
			db.end()


setTimeout fooStart, 1000