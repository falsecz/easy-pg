pg = require '../'

connection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "TestDB"

incompleteConnection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	#port: "5432"
	db:   "TestDB"

wrongConnection =
	user: "postgres"
	pswd: "123456"
	host: "localhost"
	port: "5432"
	db:   "unknown_database"

options =
	lazy: no



describe 'Immediate initialization', ->

	it 'create instance', () ->
		db = pg connection, options
		db.on 'error', -> #ignore

		assert.isObject db, 'db must be an object'
		fn = [
			'query'
			'queryAll'
			'queryOne'
			'upsert'
			'insert'
			'begin'
			'rollback'
			'commit'
			'on'
		]
		for f in fn
			assert.isFunction db[f], "must have #{f} function"

	it "emit error on incomplete connection information", (done) ->
		db = pg incompleteConnection, options
		db.on 'error', (err) ->
			done()

	it "emit error on couldn't connect", (done) ->
		db = pg wrongConnection, options
		db.on 'error', (err) ->
			done()

	it 'emit ready on successfull connection', (done) ->
		db = pg connection, options
		db.on 'error', -> #ignore
		db.on 'ready', (err) ->
			done()