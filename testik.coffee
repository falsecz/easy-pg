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

# test INSERT
INSERT_COUNT = 600
c = 1
pom = 0

###
při selhání transakce se nejprve zavolá commit, aby se uložil případný savepoint

při dokončení savepoint se může transakční fronta vymazat aý do bodu se savepointem

?NEBO SAVEPOINT VŮBEC NEŘEŠIT?

pokud je to velká chyba, tak se o nic nestarat a normálně pustit err ven k uživateli
on už si provede rollback, commit na savepoint atd

###

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