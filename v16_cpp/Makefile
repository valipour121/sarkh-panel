CXX ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wno-unused-parameter -pthread
LDLIBS = -lssl -lcrypto -lz
sarkh_engine: sarkh_engine.cpp json.hpp
	$(CXX) $(CXXFLAGS) -o $@ sarkh_engine.cpp $(LDLIBS)
clean:
	rm -f sarkh_engine
