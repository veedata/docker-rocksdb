// TO DO:
// 1. Fix the data corruption on receive issue
// 2. Setup skeleton for the multi-sync system


// Primary db is only meant to deal with put, delete and update. 
// In case of any read request, the data needs to stay with secondary db

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

// For csv
#include <chrono>
#include <ctime>
#include <fstream>


// Declaring Functions
int StartServer();
int CheckConnections();
int StopServer();
void OpenDB();
void sendToRocksDB();
void CloseDB();
void CreateDB();


// Namespaces used
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::WriteOptions;


// DB related variables
const std::string hdfsEnv = "hdfs://172.17.0.5:9000/";
const std::string kDBPrimaryPath = "primary";

DB* db_primary = nullptr; 
long my_pid = static_cast<long>(getpid());
std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
std::vector<rocksdb::ColumnFamilyHandle*> handles;


// Server related variables
#define PORT 36728      // Primary DB port
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
                // send(sd , buffer , strlen(buffer) , 0 );  

                //Close the socket and mark as 0 in list for reuse 
                close( sd );  
                client_socket[i] = 0;    
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

// Add all column families here
// 0 is lowest priority and 5 is highest priority 
const std::vector<std::string>& GetColumnFamilyNames() {
  static std::vector<std::string> column_family_names = {
      ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, "priority_0", "priority_1", "priority_2", "priority_3", "priority_4", "priority_5"};
  return column_family_names;
}

void openPrimaryDB() {

    // Connect to HDFS again and then create the table, this time can have the create_if_missing to false
    std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);

    if (!s.ok())
        fprintf(stderr, "[process %ld] Failed to open HDFS Env: %s\n", my_pid, s.ToString().c_str());
    assert(s.ok());

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = false;
    
    // Probably useless if condition, but have added it for safety. Thing is.. safety from what?
    if (nullptr == db_primary) {

        for (const auto& cf_name : GetColumnFamilyNames()) {
            column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, options));
        }

        s = DB::Open(options, kDBPrimaryPath, column_families, &handles, &db_primary);
        // s = DB::Open(options, kDBPrimaryPath, &db_primary);
        if (!s.ok()) 
            fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
        else
            fprintf(stdout, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str());
        assert(s.ok());
    }
}



void writeToCsv(std::string csv_op, std::string csv_key, std::string csv_val, std::string csv_client) {

    std::string mlbc_line = "";
    auto time_now = std::chrono::system_clock::now();
    std::time_t day_date = std::chrono::system_clock::to_time_t(time_now);
    std::string csv_day_date = std::ctime(&day_date);

    mlbc_line += csv_op + ",";
    mlbc_line += csv_key + ",";
    mlbc_line += csv_val + ",";
    mlbc_line += csv_day_date + ",";
    mlbc_line += csv_client + ",";

    std::cout<<mlbc_line<<std::endl;

    // opens file in append mode, iostream::append
    std::ofstream mlbc_dataset("./ml_dataset_primary_out.csv", std::ios::app);  
    // Write and close file
    mlbc_dataset << mlbc_line << std::endl;
    mlbc_dataset.close();
}


// Parse the buffer and convert it to rocksdb understandable functions and send to RocksDB! 
void sendToRocksDB() {

    // Using wordexp_t to strip at spaces (amongst other things that may be used if we do ldb in the future)
    wordexp_t p;
    char **w;

    wordexp(buffer, &p, 0);
    w = p.we_wordv;

    // numberOfArgs -> p.we_wordc
    // arrayOfArgs --> w or p.we_wordv

    if ((strcmp(w[0], "put") == 0) || (strcmp(w[0], "update") == 0)) {
        Status s;
        if (p.we_wordc >= 4) 
            s = db_primary->Put(WriteOptions(), handles[std::atoi(w[3])], w[1], w[2]);
            std::string csv_client = w[4];
        else
            s = db_primary->Put(WriteOptions(), w[1], w[2]);
            std::string csv_client = w[3];

        if (s.ok())
            std::cout << "Inserted key-value pair: " << w[1] << " " << w[2] << std::endl;
        else
            std::cout << "Error in inserting key and value " << w[1] << " " << w[2] << std::endl;

        std::string csv_operation = w[0];
        std::string csv_key = w[1];
        std::string csv_value = w[2];

        writeToCsv(csv_operation, csv_key, csv_value, csv_client);
    }
    else if (strcmp(w[0], "delete") == 0) {
        Status del = db_primary->SingleDelete(rocksdb::WriteOptions(), w[1]);

        if (del.ok())
            std::cout << "Deleted key " << w[1] << std::endl;
        else
            std::cout << "Error in deleting key " << w[1] << std::endl;

        std::string csv_operation = w[0];
        std::string csv_key = w[1];
        std::string csv_value = "";
        std::string csv_client = w[4];

        writeToCsv(csv_operation, csv_key, csv_value, csv_client);
    }
    else {
        std::cout << "Input error, ignoring input" << std::endl;
    }

    wordfree(&p);
}


void flushPrimaryDB() {
    Status flus = db_primary->Flush(FlushOptions());
    if (!flus.ok()) {
        fprintf(stderr, "Failed to flush DB: %s\n", flus.ToString().c_str());
        assert(false);
    }
}

void closePrimaryDB() {
    
    if (nullptr != db_primary) {

        // Remove Column Families
        for (auto h : handles) {
            delete h;
        }
        handles.clear();

        std::cout << "Trying to delete DB" <<std::endl; 
		delete db_primary;
		db_primary = nullptr;
	}
}

// Creating DB before opening it to access. This way we can have the create_if_missing option as false later
void CreateDB() {

    DB* db = nullptr;
    long my_pid = static_cast<long>(getpid());

    // To connect to the HDFS environment
    std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
    
    if (!s.ok()) 
        fprintf(stderr, "[process %ld] Failed to open HDFS env: %s\n", my_pid, s.ToString().c_str());
    else 
        printf("Opened HDFS env");

    assert(s.ok());

    Options options;
    options.env = hdfs.get();
    options.create_if_missing = true;

    // Open the DB
    s = DB::Open(options, kDBPrimaryPath, &db);

    if (!s.ok())
        fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str());
    else 
        printf("DB Open at: %s", kDBPrimaryPath.c_str());
    assert(s.ok());

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::ColumnFamilyOptions cf_opts(options);
    
    // Initialise all the CFs
    for (const auto& cf_name : GetColumnFamilyNames()) {
        if (ROCKSDB_NAMESPACE::kDefaultColumnFamilyName != cf_name) {
            rocksdb::ColumnFamilyHandle* handle = nullptr;
            s = db->CreateColumnFamily(cf_opts, cf_name, &handle);
            if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to create CF %s: %s\n", my_pid, cf_name.c_str(), s.ToString().c_str()); assert(false); }
            
            handles.push_back(handle);
        }
    }
    fprintf(stdout, "[process %ld] Column families created\n", my_pid);
    
    // Delete all the CFs
    for (auto h : handles) {
        delete h;
    }
    handles.clear();

    // Close the DB (Not destroy, just close it)
    delete db;
}

int main()
{

    // Init steps
    CreateDB();
    openPrimaryDB();
    StartServer();

    // Read from connection
    while (true)
    {   
        CheckConnections();
        buffer[0] = '\0';
    }

    closePrimaryDB();
    // Close connection
    StopServer();

    return 0;
}
