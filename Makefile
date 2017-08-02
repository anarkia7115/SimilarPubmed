all: calcScores_stream
calcScores_stream:./src/main/cpp/main_stream.cpp
	g++ -static-libgcc -static-libstdc++ -pthread -O3 -std=c++1y -o $@ $<
calcScores:./src/main/cpp/main.cpp
	g++ -g -lboost_system -lboost_filesystem -pthread -O3 -std=c++1y -o $@ $<
testQueue:./src/test/cpp/test.cpp
	g++ -g -O3 -std=c++1y ./src/test/cpp/test.cpp -o testQueue
wallet:./src/test/cpp/wallet.cpp
	g++ -pthread -O2 -std=c++1y $< -o $@
watcher:./src/test/cpp/watcher.c
	cc -o $@ $< 
sockets_server:./src/test/cpp/sockets/server.c
	cc -g -o $@ $<
sockets_client:./src/test/cpp/sockets/client.c
	cc -g -o $@ $<
shared_server:./src/test/cpp/shared/server.cpp
	g++ -g -pthread -std=c++1y -o $@ $< -lrt
shared_client:./src/test/cpp/shared/client.cpp
	g++ -g -pthread -std=c++1y -o $@ $< -lrt
simple:./src/test/cpp/simple.cpp
	g++ -std=c++11 -o $@ $<
reader:./src/test/cpp/reader.cpp
	g++ -O3 -std=c++11 -o $@ $<
