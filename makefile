.PHONY: build


build:
ifeq ($(OS),Windows_NT)
	go build -o build/searcher.exe  dex/pcsv2/v2.go dex/pcsv2/blockrazor.go
else
	go build -o build/searcher dex/pcsv2/v2.go dex/pcsv2/blockrazor.go
endif