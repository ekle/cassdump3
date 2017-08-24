run: build
	./cassdump3 -help
	@echo "-------------------------------------------"
	./cassdump3

build:
	go build -ldflags "-s -w" -o cassdump3


