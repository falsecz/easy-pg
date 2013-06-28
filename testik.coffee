pg = require "./index.coffee"

db = pg "pg://postgres:123456@localhost:5432/TestDB", lazy: no
db.on "error", (err) ->
	console.log err

db.on "ready", () ->
	console.log "DefPG Client ready..."

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

c = 0
db.query 'DELETE FROM numbers;'
foo = () ->
	c++
	insertNum c
	#getAllNames()
	#getFirstOfAllNames()
	#getNameRaw "Andrej"
	if c < 3000 then setTimeout(foo, 20)
	else db.queryOne 'SELECT count(*) FROM numbers', (err, res) ->
		console.log "inserted " + res.count + " rows of 3000"
	

setTimeout foo, 2000