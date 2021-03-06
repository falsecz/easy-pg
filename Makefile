compile:
	 ./node_modules/.bin/coffee -o lib -c src/*.coffee


test: compile test-js

publish:
	npm version patch
	git push origin master
	git push origin `git describe` #push tag
	npm publish

# all:
# 	@npm install

test-js:
	@echo '>>>>>> testing pure javascript driver'
	@./node_modules/.bin/mocha --compilers coffee:coffee-script/register --require coffee-script --require test/test_helper.coffee --reporter spec $(coffee test/test_helper.coffee)
