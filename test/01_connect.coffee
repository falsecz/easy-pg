pg = require '../'

describe 'Immediate initialization', ->

	it 'create instance', () ->
		db = pg "pg://postgres:123456@localhost:5432/TestDB", lazy: no
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
		db = pg "pg://postgres:123456@localhost:5432/unknownDB", lazy: no
		db.on 'error', (err) ->
			done()

	it 'emit ready on successfull connection', (done) ->
		db = pg "pg://postgres:123456@localhost:5432/TestDB", lazy: no
		db.on 'error', -> #ignore
		db.on 'ready', (err) ->
			done()