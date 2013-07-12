debug = require('debug') 'easy-pg-ts'

if (typeof String::startsWith != 'function') 
  String::startsWith = (str) ->
    return this.slice(0, str.length) == str

###
 Simple stack for correct transaction history handling
###
class TransactionStack

	###
	 Constructor of the stack
	 creates buffers for queries
	###
	constructor: () ->
		@stack = [] #stack for transaction
		@queue = [] #queue for trans. queries

	###
	 Returns true if this stack class is empty
	 @returns true if transaction query queue is empty
	###
	isEmpty: () =>

		return @queue.length is 0

	###
	 Flushes transaction buffers
	###
	flush: () =>
		@stack.length = 0
		@queue.length = 0

	###
	 Pushes given query object into transaction buffers
	 @requires x - queryObject
	###
	push: (x) =>
		query = x.query.split " "
		cmd = query[0]
		name = ""

		# ROLLBACK TO savepoint_name
		if query[0] is "ROLLBACK" and query[1] is "TO"
			cmd += query[1]
			name = query[2]

		switch cmd
			when "BEGIN" # push B
				@stack.unshift {type: "B", pos: @queue.length}
				@queue.push x

			when "SAVEPOINT" # push S
				@stack.unshift {type: "S", pos: @queue.length}
				@queue.push x

			when "COMMIT", "ROLLBACK" # pop until reach B
				removed = @stack.shift() while (removed?.type isnt "B" and @stack.length)
				@queue.splice removed.pos #remove all commited queries from queue

			when "ROLLBACKTO" # search required S
				for i in [0...@stack.length]
					break if @stack[i].type is "B" #search just in the last block
					if @stack[i].type is "S" and @queue[@stack[i].pos].query.indexOf(name) >= 0
						@queue.splice @stack[i].pos + 1
						@stack.splice 0, i-1
						break

			else
				@queue.push x

	###
	 Prints content of both transaction buffers
	###
	print: () =>
		console.log "[STACK]"
		for q in @stack
			console.log q
		console.log "[QUEUE]"
		for p in @queue
			console.log p


### ------- Export ------- ###

module.exports.TransactionStack = TransactionStack