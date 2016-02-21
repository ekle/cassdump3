run: build
	./cassdump -help
	@echo "-------------------------------------------"
	./cassdump

build:
	go build -o cassdump


