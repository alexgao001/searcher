build:
ifeq ($(OS),Windows_NT)
	go build -o build/searcher.exe  dex/pcsv2/v2.go
else
	go build -o build/searcher dex/pcsv2/v2.go
endif