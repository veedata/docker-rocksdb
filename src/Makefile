include make_config.mk

server: server.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ -I/usr/local/include/rocksdb -L/usr/local/lib -lrocksdb -O2 -I${HADOOP_HOME}/include -L${HADOOP_HOME}/lib/native -lhdfs $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./server
