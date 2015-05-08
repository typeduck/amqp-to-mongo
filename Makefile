compile:
	coffee -o js -c src
publish: compile
	git push && git push --tags
	npm publish
