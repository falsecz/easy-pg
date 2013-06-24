test-travis:  upgrade-pg
	# @make test-all connectionString=pg://postgres@localhost:5433/postgres

upgrade-pg:
	chmod 755 ./script/travis-pg-9.2-install.sh
	@./script/travis-pg-9.2-install.sh