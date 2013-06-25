pg = require '../'

cs = "pgsql://127.0.0.1:5432/db"

describe 'Querying', ->
	beforeEach ->
		@db = pg cs
		@db.on 'error', (err) -> console.log err #ignore

	it 'query callback called', (done) ->
		@db.query 'SET datestyle = "iso, mdy"', done

	it 'query with data callback called', (done) ->
		@db.query 'SELECT 1 WHERE 1 = $1', [1], done
