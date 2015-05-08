compile:
	coffee -o js -c src
publish: compile
	npm publish
