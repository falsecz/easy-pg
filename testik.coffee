epg = require "./index"

conObject =
	protocol:	"pg:"
	user:		"postgres"
	host:		"localhost"
	db:			"myapp_test"
	options:	{
		opt1:"x"
		opt2:"y"
		opt3:"z"
	}

client = epg conObject

client.on "ready", () -> console.log "Client is connected"

client.on "end", () -> console.log "Client is disconnected"

# not connected so far, it's deferred client!
# client creates connection right with the first query

#insert number into specified table and disconnect
client.insert "numbers", {number: 1}, (err, res) ->
	console.log err if err?
	console.log res if res?
	

client.query "select * from numbers", (err, res)->
	console.log res

client.queryOne "select * from numbers", (err, res)->
	console.log res

client.queryAll "select * from numbers", (err, res)->
	console.log res
	client.end()

#connectionStr = "pg://postgres:123456@127.0.0.1:5432/myapp_test"
#connectionStr = "pg://postgres@localhost/myapp_test"
#connectionOpts = "?lazy=yes&datestyle=iso, mdy&searchPath=public&poolSize=1"

#db = pg connectionStr+connectionOpts
###
db.on "error", (err) ->
	console.log "err: ",err

db.on "ready", () ->
	console.log "Deferred PG Client ready..."

db.on "end", (err) ->
	console.log "Client is over"


insertNum = (num) ->
	db.insert "numbers", number: num,(err, res) ->
		console.log "query fail ...", err if err
		console.log "inserted num #{num}"


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

	db.insert "numbers", [{number: 1001}, {number: 1002}, {number: 1003}], (err, res)->
		console.log err if err?
		console.log res

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

###
#setTimeout fooStart, 1000