compile:
	 ./node_modules/.bin/coffee -o lib -c src/*.coffee


test: compile test-js

publish:
	npm publish --tag 2.0.0
# all:
# 	@npm install

test-js:
	@echo '>>>>>> testing pure javascript driver'
	@./node_modules/.bin/mocha --compilers coffee:coffee-script/register --require coffee-script --require test/test_helper.coffee --reporter spec $(coffee test/test_helper.coffee)
test-native:
	@echo '>>>>>> testing native driver'
	@NATIVE=1 ./node_modules/.bin/mocha --compilers coffee:coffee-script/register --require coffee-script --require test/test_helper.coffee --reporter spec $(coffee test/test_helper.coffee)
