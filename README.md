#easy-pg
[![Build Status](https://travis-ci.org/falsecz/easy-pg.png?branch=master)](https://travis-ci.org/falsecz/easy-pg)
[![Dependency Status](https://david-dm.org/falsecz/easy-pg.png)](https://david-dm.org/falsecz/easy-pg)

easy-pg is "easy to use" deferred PostgreSQL client for node.js with possibility of using native libpq bindings and providing some frequently used querying functions. It prevents queries from not being processed due to unexpected minor errors such as temporary loss of connection. Easy-PG stacks queries during transactions as well to revive them in the case of interrupted connection.

##Installation

    npm install easy-pg

##Examples

###Simple Connection
Simple example of connecting to postgres instance, running a query and disconnecting. Client is created as deferrer client thus it's not connected until the first query is requested. In this example number <b>7</b> is inserted into table called <b>numbers</b>, column <b>number</b>. Client is disconnected right after the query result is known.

```coffeescript
epg = require "easy-pg"
# epg = require("easy-pg").native -to use native libpq bindings

client = epg "pg://postgres@localhost/myapp_test"

client.on "ready", () -> console.log "Client is connected"
client.on "end", () -> console.log "Client is disconnected"

# not connected so far, it's deferred client!
# client creates connection only with the first query

#insert number into specified table and disconnect
client.insert "numbers", {number: 7}, (err, res) ->
	console.log err if err?
	console.log res if res?
	client.end()
```
Previous code results in:

    Client is connected
    {id: 1, number: 7}
    Client is disconnected

###Connection Parameters & Options
You can pass connection string or object into Easy-PG constructor with connection options. These options are processed by client (if known) and transparently forwarded to postgres instance later.

```coffeescript
epg = require "easy-pg"

#connection string
conString = "pg://postgres@localhost/myapp_test?opt1=val1&opt2=val2&opt3=val3"

#the same connection object
conObject =
	protocol:	"pg:"
	user:		"postgres"
	host:		"localhost"
	db:			"myapp_test"

	options: {
		opt1: val1
		opt2: val2
		opt3: val3
	}

#both following is correct
client = epg conString
client = epg conObject
```

Following connection parameters and options can be used:

* Connection parameters
  * protocol <i>(required)</i>
  * user
  * password
  * host <i>(required)</i>
  * port
  * db <i>(required)</i>
* Connection options
  * lazy <i>-set to "no" or "false" to force the client to connect immediately</i>
  * datestyle <i>-instead of (in SQL) commonly used SET DATESTYLE</i>
  * searchPath <i>-instead of (in SQL) commonly used SET SEARCH_PATH</i>

Full connection string may look like this: <i>"pg://postgres:123456@localhost:5432/myapp_test?lazy=no&datestyle=iso, mdy&searchPath=public&poolSize=1"</i>, where "poolSize" is not handled by Easy-PG, but postgres instance. Connection options are checked and applied every time the client is (re)connected, thus once you for example set datestyle, it is kept set until the client is disconnected and destroyed. Even if the connection is temporarily lost.

###Client Events

There are 3 events emitted by Easy-PG client:

* ready
* end
* error <i>(client throws an ordinary Error if "error" listener is not registered, as shown in the following code)</i>

```coffeescript
epg = require "easy-pg"

client = epg "pg://postgres@localhost/myapp_test"

# an ordinary Error can be thrown here

client.on "ready", () -> console.log "Client is connected"
client.on "end", () -> console.log "Client is disconnected"

# an ordinary Error can still be thrown here

client.on "error", (err) ->
	console.log "Client error: " + err

# error event can be emitted here
```

Error event is emitted just in the case of fatal error (syntax error, etc.). For example, if postgres server is restarted while processing query and the query fails, client reconnects itself and tries to process this query again without emitting or throwing any error.
