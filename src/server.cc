// TO DO:
// 1. Fix the data corruption on receive issue
// 2. Implement column families addition
// 3. Setup skeleton for the multi-sync system

// Secondary db is only meant to deal with get and scan. 
// In case of any other request, the data needs to go to primary db

// General Libraries
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>
#include <iostream>
#include <sys/time.h>

// Secondary only - some only needed for testing
#include <cstring>
#include <thread>
#include <vector>
#include <signal.h>
#include <typeinfo>

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

// for csv
#include <fstream>
#include <chrono>
#include <ctime>


// Declaring Functions
int StartServer();
int CheckConnections();
int StopServer();
int connectToPrimaryDB();
void sendToPrimaryDB();
void disconnectPrimaryDB();
void OpenDB();
void sendToRocksDB();
void CloseDB();
void CreateDB();
std::string getSecondaryDBAddr();


using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::FlushOptions;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;

using message_size_t = uint64_t;


const std::string hdfsEnv = "hdfs://172.17.0.5:9000/";
const std::string kDBPrimaryPath = "primary";
const std::string kDBSecondaryPath = getSecondaryDBAddr();

#define PRIMARYDB_PORT 36728      // Primary DB port
#define PORT 34728                // Secondary DB port

DB *db_primary = nullptr;
char buffer[1024] = {0};
char out_char[1] = {0};
int new_socket, master_socket, addrlen, client_socket[10], max_clients = 10, activity, i, valread, sd, max_sd;
struct sockaddr_in address;
int primarydb_sock = 0;
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
    std::cout<<"Server Started!"<<std::endl;

    //clear the socket set 
    FD_ZERO(&readfds);  
    
    //add master socket to set 
    FD_SET(master_socket, &readfds);  
    max_sd = master_socket;  

    std::cout<<"Waiting for connections..."<<std::endl;

    return 0;
}

void read_bytes_internal(int sockfd, void * where, size_t size)
{
    auto remaining = size;

    while (remaining > 0) {
        // check error here
        auto just_read = recv(sockfd, where, remaining, 0);
        remaining -= just_read;
    }
} 

std::string read_message(int sockfd)
{
    message_size_t message_size;
    read_bytes_internal(sockfd, &message_size, sizeof(message_size));

    std::string result;
    read_bytes_internal(sockfd, &result, message_size);
    std::cout<<"Received: " << result <<std::endl;

    std::strcpy(buffer, result.c_str());
    return result;
}

int CheckConnections() {

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

    // wait for an activity on one of the sockets , timeout is NULL , 
    // so wait indefinitely 
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

        // send new connection greeting message 
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

            // while (strcmp(read(sd, out_char, 1), "\0") != 0) {
            //     strcat(buffer, out_char);
            // } 

            // Check if it was for closing , and also read the incoming message 
            // if ((valread = read(sd, buffer, sizeof(buffer)-1)) == 0) {
            read_message(sd);
            if (strcmp(buffer, "disco") == 0) {
            // if (strcmp(buffer, "disco") == 0) {

                //Somebody disconnected , get his details and print 
                getpeername(sd , (struct sockaddr*)&address , (socklen_t*)&addrlen);  
                printf("Host disconnected , ip %s , port %d \n", inet_ntoa(address.sin_addr) , ntohs(address.sin_port));  

                //Close the socket and mark as 0 in list for reuse 
                close( sd );  
                client_socket[i] = 0;  
            }    
            //Echo back the message that came in 
            else {
                printf("\nReceived from client: %s\n", buffer);
                sendToRocksDB();
                //set the string terminating NULL byte on the end of the data read 
                buffer[0] = '\0';
                // send(sd , buffer , strlen(buffer) , 0 );
                // buffer = {0};
                
                //Close the socket and mark as 0 in list for reuse
                // close (sd);
                // client_socket[i] = 0;  
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


int connectToPrimaryDB() {

    struct sockaddr_in primarydb_address;
    struct sockaddr_in primarydb_serv_addr;

    if ((primarydb_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("Socket creation error\n");
        return -1;
    }

    memset(&primarydb_serv_addr, '0', sizeof(primarydb_serv_addr));

    primarydb_serv_addr.sin_family = AF_INET;
    primarydb_serv_addr.sin_port = htons(PRIMARYDB_PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, "172.17.0.7", &primarydb_serv_addr.sin_addr) <= 0) {
        printf("Invalid address/ Address not supported\n"); 
        return -1;
    }

    if (connect(primarydb_sock, (struct sockaddr *)&primarydb_serv_addr, sizeof(primarydb_serv_addr)) < 0) {
        printf("Connection Failed\n"); 
        return -1;
    }

    printf("Connected to PrimaryDB!\n");

    return 0;
}


void sendToPrimaryDB() {
    // connectToPrimaryDB();
    printf("\nSending %s to PrimaryDB\n", buffer);
    int send_res = send(primarydb_sock, buffer, strlen(buffer), 0);
    if (send_res == -1){
        printf("\nError sending to primaryDB\n");
    }
    // disconnectPrimaryDB();
}


void disconnectPrimaryDB() {
    close(primarydb_sock);
}



// Random comments
// https://en.cppreference.com/w/cpp/atomic/memory_order
// https://stackoverflow.com/questions/31150809/understanding-memory-order-relaxed
// https://stackoverflow.com/questions/16294153/what-is-the-signal-function-sigint
static std::atomic<int> &ShouldSecondaryWait() {
	static std::atomic<int> should_secondary_wait{1};
	return should_secondary_wait;
}

void secondary_instance_sigint_handler(int signal) {
	ShouldSecondaryWait().store(0, std::memory_order_relaxed);
	fprintf(stdout, "\n");
	fflush(stdout);
};

// Add all column families here
// 0 is lowest priority and 5 is highest priority 
const std::vector<std::string>& GetColumnFamilyNames() {
  static std::vector<std::string> column_family_names = {
      ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, "priority_0", "priority_1", "priority_2", "priority_3", "priority_4", "priority_5"};
  return column_family_names;
}

void writeToCsv(std::string csv_op, std::string csv_key, std::string csv_val, std::string csv_client) {

    std::string mlbc_line = "";
    auto time_now = std::chrono::system_clock::now();
    std::time_t day_date = std::chrono::system_clock::to_time_t(time_now);
    char *csv_day_date_temp = std::ctime(&day_date);
    csv_day_date_temp[strcspn(csv_day_date_temp, "\r\n")] = '\0';
    std::string csv_day_date = csv_day_date_temp;

    mlbc_line += csv_op + ",";
    mlbc_line += csv_key + ",";
    mlbc_line += csv_val + ",";
    mlbc_line += csv_day_date + ",";
    mlbc_line += csv_client + ",";

    // std::cout<<mlbc_line<<std::endl;

    // opens file in append mode, iostream::append
    std::ofstream mlbc_dataset("./ml_dataset_secondary_out.csv", std::ios::app);  
    // Write L1 and close L2 file
    mlbc_dataset << mlbc_line << std::endl;
    mlbc_dataset.close();
}

void sendToRocksDB() {

    // Need to test code without this - It is basically for checking for user interrupts from what I understand
    // It additionally also makes sure that there is concurrency in code (memory_order_relaxed). So, usefulness is unknown
    ::signal(SIGINT, secondary_instance_sigint_handler);

    DB* db_secondary;

    long my_pid = static_cast<long>(getpid());
    db_secondary = nullptr;

	std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);

	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open HDFS env: %s\n", my_pid, s.ToString().c_str()); assert(false); }
    else { fprintf(stdout, "[process %ld] HDFS Open: %s\n", my_pid, s.ToString().c_str()); assert(true); }

	Options options;
	options.env = hdfs.get();
    options.create_if_missing = true;
    options.max_open_files = -1;

    std::vector<rocksdb::ColumnFamilyDescriptor> secondary_column_families;
    std::vector<rocksdb::ColumnFamilyHandle*> secondary_handles;

    for (const auto& cf_name : GetColumnFamilyNames()) {
        secondary_column_families.push_back(rocksdb::ColumnFamilyDescriptor(cf_name, options));
    }


    if (nullptr == db_secondary) {
        s = DB::OpenAsSecondary(options, kDBPrimaryPath, kDBSecondaryPath, secondary_column_families, &secondary_handles, &db_secondary);

        if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str()); assert(false); }
        else { fprintf(stdout, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str()); assert(true); }
    }

    wordexp_t p;
    char **w;

    wordexp(buffer, &p, 0);
    w = p.we_wordv;

	ReadOptions ropts;
	ropts.verify_checksums = true;
	ropts.total_order_seek = true;

    // numberOfArgs -> p.we_wordc
    // arrayOfArgs --> w
    // currently implemented: get, scan
    if (strcmp(w[0], "get") == 0) {
        
        // To be replaced by a way more complex call in the future.
        // Note: Replacement will probably come in the readData() function
        // primaryCatchUp();
        if (p.we_wordc >= 3) {
            Status catch_up = db_secondary->TryCatchUpWithPrimary(secondary_handles[std::stoi(w[2])]);
            if (!catch_up.ok()) {
                fprintf(stderr, "error while trying to catch up with primary %s\n", catch_up.ToString().c_str());
                assert(false);
            }
        } 

        std::string value;
        Status s2 = db_secondary->Get(rocksdb::ReadOptions(), w[1], &value);
        
        std::string csv_value = "";

        if (s2.ok()) {
            // std::cout << value << std::endl;
            ;
            std::string csv_value = value;
        }
        else {
            std::cout << "Error in locating value for key " << w[1] << s2.ToString().c_str() << std::endl;
        }
        
        std::string csv_operation = w[0];
        std::string csv_key = w[1];
        std::string csv_client = w[3];

        // writeToCsv(csv_operation, csv_key, csv_value, csv_client);
    }
    else if (strcmp(w[0], "scan") == 0) {
        rocksdb::Iterator *it = db_secondary->NewIterator(ReadOptions());
		int count = 0;

		for (it->SeekToFirst(); it->Valid(); it->Next()) {
			count++;
            // std::cout << it->key().ToString() << std::endl;

            std::string csv_operation = w[0];
            std::string csv_key = it->key().ToString();
            std::string csv_client = w[3];
            std::string csv_value = it->value().ToString();

            // writeToCsv(csv_operation, csv_key, csv_value, csv_client);
		}

		fprintf(stdout, "Observed %i keys\n", count); 
    }
    else if ((strcmp(w[0], "put") == 0) || (strcmp(w[0], "update") == 0) || (strcmp(w[0], "delete") == 0)) {
        // std::cout << "Sending to PrimaryDB" << std::endl;
        sendToPrimaryDB();
    }
    else {
        std::cout << "Input error, ignoring input" << std::endl;
    }

    wordfree(&p);

    if (nullptr != db_secondary) {
		delete db_secondary;
		db_secondary = nullptr;
	}
}


std::string getSecondaryDBAddr() {

    std::string curr_path = "secondary/";

    char hostname[HOST_NAME_MAX] = {0};
    gethostname(hostname, HOST_NAME_MAX);

    curr_path += hostname;

    return curr_path;
}

void CreateDB() {

    DB *db = nullptr;
    std::cout << "Trying to open db at: " << kDBSecondaryPath << " end" << std::endl; 
	long my_pid = static_cast<long>(getpid());
	
	std::unique_ptr<rocksdb::Env> hdfs;
    Status s = rocksdb::NewHdfsEnv(hdfsEnv, &hdfs);
	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open HDFS env: %s\n", my_pid, s.ToString().c_str()); assert(false); }
    else { fprintf(stdout, "[process %ld] HDFS Open: %s\n", my_pid, s.ToString().c_str()); assert(true); }

	Options options;
	options.env = hdfs.get();
    options.create_if_missing = true;
    options.max_open_files = -1;

    s = DB::OpenAsSecondary(options, kDBPrimaryPath, kDBSecondaryPath, &db);

	if (!s.ok()) { fprintf(stderr, "[process %ld] Failed to open DB: %s\n", my_pid, s.ToString().c_str()); assert(false); }
    else { fprintf(stdout, "[process %ld] DB Open: %s\n", my_pid, s.ToString().c_str()); assert(true); }

    delete db;
}

int main() {

    // Init steps
    // connectToPrimaryDB();
    CreateDB();
    connectToPrimaryDB();
    StartServer();

    // Read from connection
    while (true)
    {   
        CheckConnections();
        buffer[0] = '\0';
    }

    // Close connection
    // Currently should not be reached!
    disconnectPrimaryDB();
    StopServer();

    return 0;
}
