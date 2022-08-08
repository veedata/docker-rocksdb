#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

#include <wordexp.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#include "plugin/hdfs/env_hdfs.h"
#include "hdfs.h"


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;

const std::string hdfsEnv = "hdfs://172.17.0.5:9000/";
const std::string kDBPrimaryPath = "vee/ported/prim";
const std::string kDBSecondaryPath = "vee/ported/sec";

DB *db = nullptr;

#define PORT 36728

char buffer[1024] = {0};
// std::string buffer;
int server_fd, new_socket;
struct sockaddr_in address;
int addrlen = sizeof(address);


int startServer() {
    int opt = 1;
    // char *hello = "Hello from server";

    // Creating socket file descriptor
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    // Forcefully attaching socket to the port
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Forcefully attaching socket to the port
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }
    new_socket = accept(server_fd, (struct sockaddr *)&address, (socklen_t *)&addrlen);
    if (new_socket < 0) {
        perror("accept");
        exit(EXIT_FAILURE);
    }

    return 0;
}

int sendToRocksDB() {

    // std::cout << buffer << std::endl;

    wordexp_t p;
    char **w;

    wordexp(buffer, &p, 0);
    w = p.we_wordv;

    // int i = 0;

    // for (i = 0; i < p.we_wordc; i++)
    //     printf("%i: %s\n", i, w[i]);


    // numberOfArgs -> p.we_wordc
    // arrayOfArgs --> w

    // for now only impementing get and put, will expand in the future
    if ( strcmp(w[1], "put") == 0 ) {
        Status s = db->Put(WriteOptions(), "game0", "apex");
        assert(s.ok());
    }
    else if ( strcmp(w[1], "get") == 0 ) {
        std::string value;
        Status s2 = db->Get(rocksdb::ReadOptions(), "game0", &value);
        assert(s2.ok());

        std::cout << value << std::endl;
    }
    else {
        std::cout << "NOTA" << std::endl;
    }

    // // Future idea!!!
    // ROCKSDB_NAMESPACE::LDBTool tool;
    // tool.Run(i, w);

    wordfree(&p);

    return 0;
}


int readData() {
    int valread;
    valread = read(new_socket, buffer, 1024);

    if (buffer[0] != '\0') {
        sendToRocksDB();
    }

    // buffer[0] = '\0';
    memset(&buffer[0], 0, sizeof(buffer));

    return 0;
}

int stopServer() {
    // closing the connected socket
    close(new_socket);
    // closing the listening socket
    shutdown(server_fd, SHUT_RDWR);

    return 0;
}

void CreateDB() {
	long my_pid = static_cast<long>(getpid());
	
	std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str()); assert(false); }

	Options options;
	options.env = hdfs.get();
	options.create_if_missing = true;

	s = DB::Open(options, kDBPrimaryPath, &db);
	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str()); assert(false); }
    else { fprintf(stderr, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str()); assert(true); }
}

void RemoveDB() {
	long my_pid = static_cast<long>(getpid());

	std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str()); assert(false); }

    Options options;
	options.env = hdfs.get();
	options.create_if_missing = true;

    s = DestroyDB(kDBPrimaryPath, options);
}


int main() {

    // Init steps
    startServer();
    CreateDB();

    // Read from connection
    while(true) {
        buffer[0] = '\0';
        readData();
    }

    // Close connection
    // Currently should not be reached!
    stopServer();
    RemoveDB();

    return 0;
}
