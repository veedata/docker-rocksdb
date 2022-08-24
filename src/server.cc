// Primary db is only meant to deal with put, delete and update. 
// In case of any other request, the data needs to go to secondary db

// General Libraries
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <iostream>
#include <sys/time.h>

// RocksDB Libraries
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

// HDFS Libraries
#include "plugin/hdfs/env_hdfs.h"
#include "hdfs.h"

// Server Libraries
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <wordexp.h>
#include <arpa/inet.h>


// Declaring Functions
int StartServer();
int CheckConnections();
int StopServer();
void OpenDB();
void sendToRocksDB();
void CloseDB();
void CreateDB();



using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;



const std::string hdfsEnv = "hdfs://172.17.0.5:9000/";
const std::string kDBPrimaryPath = "primary";
const std::string kDBSecondaryPath = "secondary";

#define PORT 36728      // Primary DB port
// #define PORT 34728   // Secondary DB port

DB* db = nullptr;
char buffer[1024] = {0};
int new_socket, master_socket, addrlen, client_socket[10], max_clients = 10, activity, i, valread, sd, max_sd;
struct sockaddr_in address;
fd_set readfds;



int StartServer() {
    int opt = 1;

    //initialise all client_socket[] to 0 so not checked 
    for (i = 0; i < max_clients; i++) {
        client_socket[i] = 0;
    }

    //create a master socket 
    if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {  
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
     
    //set master socket to allow multiple connections , 
    //this is just a good habit, it will work without this 
    if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0 )  {  
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    // Forcefully attaching socket to the port
    if (bind(master_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(master_socket, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    // Accept the incoming connection 
    addrlen = sizeof(address);
    puts("Server Started!");
    puts("Waiting for connections...");

    return 0;
}

int CheckConnections() {

    //clear the socket set 
    FD_ZERO(&readfds);  
    
    //add master socket to set 
    FD_SET(master_socket, &readfds);  
    max_sd = master_socket;  
            
    //add child sockets to set 
    for ( i = 0 ; i < max_clients ; i++) {
        //socket descriptor 
        sd = client_socket[i];

        //if valid socket descriptor then add to read list 
        if(sd > 0)
            FD_SET( sd , &readfds);

        //highest file descriptor number, need it for the select function 
        if(sd > max_sd)
            max_sd = sd;
    }

    //wait for an activity on one of the sockets , timeout is NULL , 
    //so wait indefinitely 
    activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL);  

    if ((activity < 0) && (errno!=EINTR)) {  
        printf("select error");
    }

    // If something happened on the master socket, then its an incoming connection 
    if (FD_ISSET(master_socket, &readfds)) {

        if ((new_socket = accept(master_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen))<0) {  
            perror("accept");  
            exit(EXIT_FAILURE);  
        }

        // inform user of socket number - used in send and receive commands 
        printf("New connection , socket fd is %d , ip is : %s , port : %d\n" , new_socket , inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

        //send new connection greeting message 
        // if (send(new_socket, message, strlen(message), 0) != strlen(message)) {  
        //     perror("send");  
        // }  

        //add new socket to array of sockets 
        for (i = 0; i < max_clients; i++) {  
            //if position is empty 
            if( client_socket[i] == 0 ) {
                client_socket[i] = new_socket;  
                printf("Adding to list of sockets as %d\n" , i);
                break;
            }
        }
    }

    // else its some IO operation on some other socket
    for (i = 0; i < max_clients; i++) {

        sd = client_socket[i];  
                
        if (FD_ISSET( sd , &readfds)) {

            // Check if it was for closing , and also read the incoming message 
            if ((valread = read(sd ,buffer, 1024)) == 0) {

                //Somebody disconnected , get his details and print 
                getpeername(sd , (struct sockaddr*)&address , (socklen_t*)&addrlen);  
                printf("Host disconnected , ip %s , port %d \n", inet_ntoa(address.sin_addr) , ntohs(address.sin_port));  

                //Close the socket and mark as 0 in list for reuse 
                close( sd );  
                client_socket[i] = 0;  
            }    
            //Echo back the message that came in 
            else {
                sendToRocksDB();
                //set the string terminating NULL byte on the end of the data read 
                buffer[valread] = '\0';  
                send(sd , buffer , strlen(buffer) , 0 );  
            }  
        }
    }

    return 0;
}

int StopServer() {
    // closing the connected socket
    close(new_socket);
    // closing the listening socket
    shutdown(master_socket, SHUT_RDWR);

    return 0;
}

void OpenDB() {
    long my_pid = static_cast<long>(getpid());
    db = nullptr;

    std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
    

    if (!s.ok()) {
        fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
        assert(false);
    }

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = false;

    if (nullptr == db) {
        s = DB::Open(options, kDBPrimaryPath, &db);
        if (!s.ok()) 
            fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
        else
            fprintf(stdout, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str());
        assert(s.ok());
    }
}

void sendToRocksDB()
{
    // if (nullptr != db) {
    //     OpenDB();
	// }

    DB* db_primary; 

    long my_pid = static_cast<long>(getpid());
    db_primary = nullptr;

    std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
    

    if (!s.ok()) {
        fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
        assert(false);
    }

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = false;

    if (nullptr == db_primary) {
        s = DB::Open(options, kDBPrimaryPath, &db_primary);
        if (!s.ok()) 
            fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
        else
            fprintf(stdout, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str());
        assert(s.ok());
    }

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
            s = db_primary->Put(WriteOptions(), w[1], w[2], w[3]);
        else
            s = db_primary->Put(WriteOptions(), w[1], w[2]);

        if (s.ok())
            std::cout << "Inserted key-value pair: " << w[1] << " " << w[2] << std::endl;
        else
            std::cout << "Error in inserting key and value " << w[1] << " " << w[2] << std::endl; 
    }
    else if (strcmp(w[0], "delete") == 0) {
        Status del = db_primary->SingleDelete(rocksdb::WriteOptions(), w[1]);

        if (del.ok())
            std::cout << "Deleted key " << w[1] << std::endl;
        else
            std::cout << "Error in deleting key " << w[1] << std::endl;
    }
    else if (strcmp(w[0], "get") == 0) {
        
        // To be replaced by a way more complex call in the future.
        // Note: Replacement will probably come in the readData() function
        // primaryCatchUp();

        std::string value;
        Status s2 = db_primary->Get(rocksdb::ReadOptions(), w[1], &value);
        
        if (s2.ok())
            std::cout << value << std::endl;
        else
            std::cout << "Error in locating value for key " << w[1] << std::endl;
    }
    else if (strcmp(w[0], "scan") == 0) {
        rocksdb::Iterator *it = db_primary->NewIterator(ReadOptions());
		int count = 0;

		for (it->SeekToFirst(); it->Valid(); it->Next()) {
			count++;
            std::cout << it->key().ToString() << std::endl;
		}
		
		fprintf(stdout, "Observed %i keys\n", count); 
    }
    else {
        std::cout << "Input error, ignoring input" << std::endl;
    }

    Status flus = db_primary->Flush(FlushOptions());
    if (!flus.ok()) {
        fprintf(stderr, "Failed to flush DB: %s\n", flus.ToString().c_str());
        assert(false);
    }

    wordfree(&p);

    // if (nullptr != db) {
    //     CloseDB();
	// }


    if (nullptr != db_primary) {
        std::cout << "Trying to delete DB" <<std::endl; 
		delete db_primary;
		db_primary = nullptr;
	}
}

void CloseDB() {
    if (nullptr != db) {
        std::cout << "Trying to delete DB" <<std::endl; 
		delete db;
		db = nullptr;
	}
}

void CreateDB() {
    long my_pid = static_cast<long>(getpid());

    std::unique_ptr<rocksdb::Env> hdfs;
    Status hdfsopen = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
    
    if (!hdfsopen.ok()) 
        fprintf(stderr, "[process %ld] Failed to open HDFS env: %s\n", my_pid, hdfsopen.ToString().c_str());
    else 
        printf("Opened HDFS env");

    assert(hdfsopen.ok());

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = true;

    // Status s = ROCKSDB_NAMESPACE::DestroyDB(kDBPrimaryPath, options);
    // if (!s.ok()) {
    //     fprintf(stderr, "[process %ld] Failed to destroy DB: %s\n", my_pid, s.ToString().c_str());
    //     assert(false);
    // }

    // hdfsopen = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
    
    if (!hdfsopen.ok()) 
        fprintf(stderr, "[process %ld] Failed to open HDFS env: %s\n", my_pid, hdfsopen.ToString().c_str());
    else 
        printf("Opened HDFS env part 2 \n");

    db = nullptr;
    Status s = DB::Open(options, kDBPrimaryPath, &db);
    if (!s.ok())
        fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
    else 
        printf("DB Open at: %s", kDBPrimaryPath.c_str());

    assert(s.ok());

    delete db;
}

int main()
{

    // Init steps
    CreateDB();
    StartServer();

    // Read from connection
    while (true)
    {   
        CheckConnections();
        buffer[0] = '\0';
    }

    // Close connection
    StopServer();

    return 0;
}
