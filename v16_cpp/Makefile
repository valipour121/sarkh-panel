CXX ?= g++
CXXFLAGS ?= -O2 -std=c++17 -Wall -Wextra -Wno-unused-parameter -pthread
LDLIBS = -lssl -lcrypto -lz
sarkh_engine: sarkh_panel_v16_cpp.cpp json.hpp
	$(CXX) $(CXXFLAGS) -o $@ sarkh_panel_v16_cpp.cpp $(LDLIBS)
clean:
	rm -f sarkh_engine
