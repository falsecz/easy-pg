{exec} = require "child_process"
pg = require "../"

connectionStr = "pg://postgres@localhost/myapp_test?poolSize=5"

describe "Connection options with pooling", ->
	@timeout 10000 # ms

	schemaName = "pool_test_schema"

	db = pg connectionStr

	before (done) ->
		db.query "CREATE SCHEMA IF NOT EXISTS #{schemaName}", done

	after (done) ->
		db.query "DROP SCHEMA IF EXISTS #{schemaName}", done

	it "initializes every connection using same options", (done) ->

		# due to complex initialization, this test must be run without any previous pg usage

		cmd = "coffee #{__dirname}/06_connection_options/test.coffee \"#{connectionStr}&searchPath=#{schemaName}\" #{schemaName}"
		exec cmd, (err, stdout, stderr) ->
			return done(stderr or err) if err
			done()
