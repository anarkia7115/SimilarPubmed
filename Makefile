all: calcScores testQueue wallet
calcScores:./src/main/cpp/main.cpp
	g++ -pthread -O3 -std=c++1y ./src/main/cpp/main.cpp -o calcScores
testQueue:./src/test/cpp/test.cpp
	g++ -g -O3 -std=c++1y ./src/test/cpp/test.cpp -o testQueue
wallet:./src/test/cpp/wallet.cpp
	g++ -pthread -O2 -std=c++1y $< -o $@

