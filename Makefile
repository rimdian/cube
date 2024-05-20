dev:
	@echo "Launching dev cubejs"
	yarn run dev

build:
	@echo "Build server"
	# docker build . --file Dockerfile --progress=plain --no-cache --platform linux/amd64
	docker build . --file Dockerfile --progress=plain --platform linux/amd64