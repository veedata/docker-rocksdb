// Primary db is only meant to deal with put, delete and update. 
// In case of any other request, the data needs to go to secondary db


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
const std::string kDBPrimaryPath = "primary";
// const std::string kDBSecondaryPath = "vee/ported/sec";

DB *db = nullptr;

#define PORT 36728      // Primary DB port
// #define PORT 34728   // Secondary DB port

char buffer[1024] = {0};
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

int sendToRocksDB()
{
    wordexp_t p;
    char **w;

    wordexp(buffer, &p, 0);
    w = p.we_wordv;

    // numberOfArgs -> p.we_wordc
    // arrayOfArgs --> w
    // currently implemented: get, put, scan, delete, update
    if ((strcmp(w[0], "put") == 0) || (strcmp(w[0], "update") == 0)) {
        Status s;
        if (p.we_wordc >= 4) 
            s = db->Put(WriteOptions(), w[1], w[2], w[3]);
        else
            s = db->Put(WriteOptions(), w[1], w[2]);

        if (s.ok())
            std::cout << "Inserted key-value pair" << std::endl;
        else
            std::cout << "Error in inserting key and value" << w[1] << w[2] << std::endl; 
    }
    else if (strcmp(w[0], "delete") == 0) {
        Status del = db->SingleDelete(rocksdb::WriteOptions(), w[1]);

        if (del.ok())
            std::cout << "Deleted key " << w[1] << std::endl;
        else
            std::cout << "Error in deleting key " << w[1] << std::endl;
    }
    else {
        std::cout << "Input error, ignoring input" << std::endl;
    }

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

    while(true) {
        // Read from connection
        buffer[0] = '\0';
        readData();
    }

    // Close connection
    // Currently should not be reached!
    stopServer();
    RemoveDB();

    return 0;
}
