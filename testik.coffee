pg = require "./index.coffee"

connection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "TestDB"

options =
	lazy: no

db = pg connection, options

db.on "error", (err) ->
	console.log err

db.on "ready", () ->
	console.log "Deferred PG Client ready..."


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


insertName "David", "Gosh"

###getName "Andrej"
getAllNames()
getFirstOfAllNames()
getNameRaw "Andrej"###

#clear db-table numbers
db.query 'DROP TABLE IF EXISTS numbers;', (err, res) ->
		console.log "DROP TABLE query fail ...", err if err

db.query "CREATE TABLE IF NOT EXISTS numbers (_id bigserial primary key, number int NOT NULL);", (err, res) ->
		console.log "CREATE TABLE query fail ...", err if err

# test INSERT
INSERT_COUNT = 100
c = 1
foo = () ->
	insertNum c
	return setTimeout(foo, 20) if c++ < INSERT_COUNT

	#in the end
	db.queryOne 'SELECT count(*) FROM numbers', (err, res) ->
		return console.log err if err
		console.log "OK!" if (parseInt res.count, 10) is INSERT_COUNT
	

setTimeout foo, 2000