debug = require('debug') 'easy-pg-ts'

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
	@returns  true if transaction query stack is empty
	###
	isEmpty: () =>

		return @stack.length is 0

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
		if Array.isArray x.query #in the case of QuerySequence
			cmd = ""
		else
			query = x.query.toUpperCase().trim().split " ", 2
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

			when "COMMIT" # pop until reach B
				@stack.shift() while (removed?.type isnt "B" and @stack.length)
				@queue.push x
				@flush() if @isEmpty() #last commit clears query queue

			when "ROLLBACK" # pop until reach B
				removed = @stack.shift() while (removed?.type isnt "B" and @stack.length)
				@queue.splice removed.pos #remove all rolled back queries from queue

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
	toString: () =>
		res = "[STACK]\n"
		
		for q in @stack
			res += "#{q.type} : #{q.pos}\n"

		res += "[QUEUE]\n"

		for p in @queue
			res += "#{p.toString()}\n"

		return res


### ------- Export ------- ###

module.exports.TransactionStack = TransactionStack
