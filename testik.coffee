pg = require "./index.coffee"
{TransactionStack} = require "#{__dirname}/transactionStack.coffee"

connectionStr = "pg://postgres:123456@localhost:5432/TestDB?lazy=no&opt1=ANDF_011'"
connection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "TestDB"

options =
	lazy: no

#db = pg connection, options
db = pg connectionStr, options

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

###
ts = new TransactionStack()
ts.push {query: "BEGIN"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "BEGIN"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "SAVEPOINT fx"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "BEGIN"}
ts.push {query: "COMMIT"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "ROLLBACK TO fx"}
ts.push {query: "ROLLBACK TO fx"}
ts.push {query: "ROLLBACK TO fx"}
ts.push {query: "ROLLBACK TO fx"}
ts.push {query: "INSERT x INTO y"}
ts.push {query: "INSERT x INTO y"}
ts.toString()
ts.push {query: "COMMIT"}
ts.toString()
###

# test INSERT
INSERT_COUNT = 1000
c = 1
pom = 0


db.begin () ->
	console.log "transaction begin"

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
		db.end()


	
foo2 = () ->
	console.log "db.rollback()"
	clearTimeout pom
	db.rollback "x20savepoint", () ->
		console.log "rolled back"
		#db.end()

setTimeout foo, 1000
#setTimeout foo2, 1500
#setTimeout foo, 2000