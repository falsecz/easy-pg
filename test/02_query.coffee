pg = require '../'

cs = "pg://postgres:123456@localhost:5432/TestDB"

describe 'Querying', ->
	beforeEach ->
		@db = pg cs, lazy: no
		@db.on 'error', (err) -> console.log err #ignore

	describe 'query', ->

		it 'query callback called', (done) ->
			@db.query 'SET datestyle = "iso, mdy"', done

		it 'query with data callback called', (done) ->
			@db.query 'SELECT 1 WHERE 1 = $1', [1], done

		it 'query with long data callback called', (done) ->
			@db.query """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], done

	describe 'queryOne', ->
		it 'callback called', (done) ->
			@db.queryOne 'SET datestyle = "iso, mdy"', done

		it 'with data callback called', (done) ->
			@db.queryOne 'SELECT 1 WHERE 1 = $1', [1], done

		it 'with long data callback called', (done) ->
			@db.queryOne """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], done

	describe 'queryAll', ->
		it 'callback called', (done) ->
			@db.queryAll 'SET datestyle = "iso, mdy"', done

		it 'with data callback called', (done) ->
			@db.queryAll 'SELECT 1 WHERE 1 = $1', [1], done

		it 'with long data callback called', (done) ->
			@db.queryAll """
				SELECT 1 WHERE 1 = $1 OR 1 = $2 OR 1 = $3 OR 1 = $4
			""", [1, 1, 1, 1], done

