pg = require "../"

connOpts = "?lazy=no&dateStyle=iso, mdy&searchPath=public&poolSize=5"
connectionStr = "pg://postgres@localhost/myapp_test" + connOpts

describe "Async Transactions", ->
	@timeout 10000 # 10sec
	db = null

	beforeEach ->
		if db is null
			db = pg connectionStr
			db.on 'error', (err) ->
				console.log err

	it "deferred foreign key constraint #9", (done) ->
		db.query "DROP TABLE IF EXISTS a"
		db.query "DROP TABLE IF EXISTS b"
		db.query "CREATE TABLE a (id serial primary key, b_id int NOT NULL)"
		db.query "CREATE TABLE b (id serial primary key, a_id int NOT NULL)"
		db.query "ALTER TABLE a ADD CONSTRAINT a_b_id FOREIGN KEY (b_id) REFERENCES b (id) DEFERRABLE"
		db.begin()
		db.query 'SET CONSTRAINTS a_b_id DEFERRED'

		db.insert 'a', {id: 1, b_id: 11}
		db.insert 'b', {id: 11, a_id: 1}
		db.commit done

	it "deferred foreign key constraint #9 - async", (done) ->
		async = require 'async'
		async.waterfall [
			(cb) -> db.query "DROP TABLE IF EXISTS a", cb
			(res, cb) -> db.query "DROP TABLE IF EXISTS b", cb
			(res, cb) -> db.query "CREATE TABLE a (id serial primary key, b_id int NOT NULL)", cb
			(res, cb) -> db.query "CREATE TABLE b (id serial primary key, a_id int NOT NULL)", cb
			(res, cb) -> db.query "ALTER TABLE a ADD CONSTRAINT a_b_id FOREIGN KEY (b_id) REFERENCES b (id) DEFERRABLE", cb
			(res, cb) -> db.begin cb
			(res, cb) -> db.query 'SET CONSTRAINTS a_b_id DEFERRED', cb
			(res, cb) -> db.insert 'a', {id: 1, b_id: 11}, cb
			(res, cb) -> db.insert 'b', {id: 11, a_id: 1}, cb
			(res, cb) -> db.commit cb
		], done

