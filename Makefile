CXX ?= c++
CXXFLAGS ?= -O2 -Wall -Wextra -pedantic
ifeq ($(shell uname -s),Darwin)
CPPFLAGS += -I$(shell brew --prefix nlohmann-json 2>/dev/null)/include
endif

.PHONY: build test
build: bin/loadgen
	mkdir -p bin
	go build -buildvcs=false -o bin/gateway ./cmd/gateway
	go build -buildvcs=false -o bin/simulator ./cmd/simulator

bin/loadgen: loadgen/main.cpp loadgen/stream.hpp
	mkdir -p bin
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 $< -o $@ -lcurl -pthread

test:
	go test -buildvcs=false -race ./...
	mkdir -p bin
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++20 loadgen/stream_test.cpp -o bin/stream_test
	./bin/stream_test
