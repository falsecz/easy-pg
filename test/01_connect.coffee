pg = require '../'

describe 'Initialization', ->

	it 'create instance', () ->
		db = pg "pgsql://127.0.0.1:5432/db"
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


	it "emit error on couldn't connect", (done) ->
		db = pg "pgsql://9.9.9.9:5432/db"
		db.on 'error', (err) ->
			done()
			# console.log err

	it 'emit ready on successfull connection', (done) ->
		db = pg "pgsql://127.0.0.1:5432/db"
		db.on 'error', -> #ignore
		db.on 'ready', (err) ->
			done()


