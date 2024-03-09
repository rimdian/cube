dev:
	@echo "Launching dev cubejs"
	yarn run dev

build:
	@echo "Build server"
	# docker build . --file Dockerfile --progress=plain --no-cache
	docker build . --file Dockerfile --progress=plain