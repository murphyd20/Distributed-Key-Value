// The key-value server implementation

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>

#include "defs.h"
#include "hash.h"
#include "util.h"


// Program arguments

// Host name and port number of the metadata server
static char mserver_host_name[HOST_NAME_MAX] = "";
static uint16_t mserver_port = 0;

// Ports for listening to incoming connections from clients, servers and mserver
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;
static uint16_t mservers_port = 0;

// Current server id and total number of servers
static int server_id = -1;
static int num_servers = 0;

// Log file name
static char log_file_name[PATH_MAX] = "";


static void usage(char **argv)
{
	printf("usage: %s -h <mserver host> -m <mserver port> -c <clients port> -s <servers port> "
	       "-M <mservers port> -S <server id> -n <num servers> [-l <log file>]\n", argv[0]);
	printf("If the log file (-l) is not specified, log output is written to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "h:m:c:s:M:S:n:l:")) != -1) {
		switch(option) {
			case 'h': strncpy(mserver_host_name, optarg, HOST_NAME_MAX); break;
			case 'm': mserver_port  = atoi(optarg); break;
			case 'c': clients_port  = atoi(optarg); break;
			case 's': servers_port  = atoi(optarg); break;
			case 'M': mservers_port = atoi(optarg); break;
			case 'S': server_id     = atoi(optarg); break;
			case 'n': num_servers   = atoi(optarg); break;
			case 'l': strncpy(log_file_name, optarg, PATH_MAX); break;
			default:
				fprintf(stderr, "Invalid option: -%c\n", option);
				return false;
		}
	}

	return (mserver_host_name[0] != '\0') && (mserver_port != 0) && (clients_port != 0) && (servers_port != 0) &&
	       (mservers_port != 0) && (num_servers >= 3) && (server_id >= 0) && (server_id < num_servers);
}


// Socket for sending requests to the metadata server
static int mserver_fd_out = -1;
// Socket for receiving requests from the metadata server
static int mserver_fd_in = -1;

// Sockets for listening for incoming connections from clients, servers and mserver
static int my_clients_fd = -1;
static int my_servers_fd = -1;
static int my_mservers_fd = -1;

// Store fds for all connected clients, up to MAX_CLIENT_SESSIONS
#define MAX_CLIENT_SESSIONS 1000
static int client_fd_table[MAX_CLIENT_SESSIONS];

// Store fds for connected servers
static int server_fd_table[2] = {-1, -1};


// Storage for primary key set
hash_table primary_hash = {0};

// Storage for secondary key set
hash_table secondary_hash = {0};


// Primary server (the one that stores the primary copy for this server's secondary key set)
static int primary_sid = -1;
static int primary_fd = -1;

// Secondary server (the one that stores the secondary copy for this server's primary key set)
static int secondary_sid = -1;
static int secondary_fd = -1;

bool switched_to_primary = false;

//---------------------------Methods-----------------------------------
int heart_stopped = 0;

void *heart_beat()
{
	while(!heart_stopped && mserver_fd_out >= 0){

		log_write("%s Heartbeat, Pump pump\n", current_time_str());

		char send_buffer[MAX_MSG_LEN] = {0};

		mserver_ctrl_request *heart_beat_request = (mserver_ctrl_request*)send_buffer;

		heart_beat_request->hdr.type = MSG_MSERVER_CTRL_REQ;		
		heart_beat_request->type = HEARTBEAT;
		heart_beat_request->server_id = server_id;

		send_msg(mserver_fd_out, heart_beat_request, sizeof(*heart_beat_request));
		usleep(333000);
		
	}
	return NULL;
}

//Note: I may have mixed up the iteration functions below, but they still work as intended.

// Hash Iterator Function which sends across a key from either it's primary or secondary key set.
// If the key belongs in this server, then this function will send the key-value of to the Secondary Server
// If the key belongs to the elsewhere, then this function will 
//		send the key-value to the Primary server 
static void send_key_value(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	char req_buffer[MAX_MSG_LEN] = {0};
	char recv_buffer[MAX_MSG_LEN] = {0};
	
	//ADD Lock

	int key_srv_id = key_server_id(key, num_servers);
	int serverFD;

	//log_write("key_srv_id[%d]\n",key_srv_id);

	if(key_srv_id == server_id){
		log_write("\tsecondary_fd\n");
		//LOCK
		hash_lock(&secondary_hash, key);
		serverFD = secondary_fd;
		}
	else{
		log_write("\tprimary_fd\n");
		hash_lock(&primary_hash, key);
		serverFD = primary_fd;
	}

	operation_request *request = (operation_request*)req_buffer;

	request->type = OP_PUT;
	request->hdr.type = MSG_OPERATION_REQ;
	
	memcpy(request->key, key, KEY_SIZE);
	strncpy(request->value, value, value_sz);
		
	if(!send_msg(serverFD, request, sizeof(*request) + value_sz) || 
		!recv_msg(serverFD, recv_buffer, sizeof(recv_buffer), MSG_OPERATION_RESP))
	{
			strncpy(arg, "False", 5);
//			log_write("ACTUALLY FALSE\n");
	}
	else{
		strncpy(arg, "True", 4);
//		log_write("ACTUALLY TRUE\n");
	}
	if(key_srv_id == server_id)
		hash_unlock(&secondary_hash, key);
	else
		hash_unlock(&primary_hash, key);
}

void *send_primary_hash(){
	char result[6];
	char req_buffer[MAX_MSG_LEN] = {0};
	
	mserver_ctrl_request *request = (mserver_ctrl_request *) req_buffer;
	request->hdr.type = MSG_MSERVER_CTRL_REQ;
	request->server_id = primary_sid;
	
	hash_iterate(&secondary_hash, send_key_value, result);
	char str_tru[5] = "True";
	if(strncmp(str_tru, result, 4) == 0){
		request->type = UPDATED_PRIMARY;
		}
	else
		request->type = UPDATE_PRIMARY_FAILED;
		
	send_msg(mserver_fd_out, request, sizeof(*request));

	return NULL;
}


void *send_secondary_hash(){
	char result[6];
	char req_buffer[MAX_MSG_LEN] = {0};
	
	mserver_ctrl_request *request = (mserver_ctrl_request *) req_buffer;
	request->hdr.type = MSG_MSERVER_CTRL_REQ;
	request->server_id = secondary_sid;

	// Iterate over the primary Hash	
	hash_iterate(&primary_hash, send_key_value, result);

	char str_tru[5] = "True";

	if(strncmp(str_tru, result, 4) == 0){
		request->type = UPDATED_SECONDARY;
		}
	else
		request->type = UPDATE_SECONDARY_FAILED;
		
	// Send Metadata Contral Message back to Metadata Server
	send_msg(mserver_fd_out, request, sizeof(*request));
	
	return NULL;
}




pthread_t heart_beat_thread;

pthread_t primary_update_thread;

pthread_t secondary_update_thread;

static void cleanup();

static const int hash_size = 65536;

// Initialize and start the server
static bool init_server()
{
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		client_fd_table[i] = -1;
	}

	// Get the host name that server is running on
	char my_host_name[HOST_NAME_MAX] = "";
	if (get_local_host_name(my_host_name, sizeof(my_host_name)) < 0) {
		return false;
	}
	log_write("%s Server starts on host: %s\n", current_time_str(), my_host_name);

	// Create sockets for incoming connections from clients and other servers
	if (((my_clients_fd  = create_server(clients_port, MAX_CLIENT_SESSIONS, NULL)) < 0) ||
	    ((my_servers_fd  = create_server(servers_port, 2, NULL)) < 0) ||
	    ((my_mservers_fd = create_server(mservers_port, 1, NULL)) < 0))
	{
		goto cleanup;
	}

	// Connect to mserver to "register" that we are live
	if ((mserver_fd_out = connect_to_server(mserver_host_name, mserver_port)) < 0) {
		goto cleanup;
	}

	// Determine the ids of replica servers
	primary_sid = primary_server_id(server_id, num_servers);
	secondary_sid = secondary_server_id(server_id, num_servers);

	// Initialize key-value storage
	if (!hash_init(&primary_hash, hash_size)) {
		goto cleanup;
	}

	if (!hash_init(&secondary_hash, hash_size)) {
		goto cleanup;
	}
	
	if(pthread_create(&heart_beat_thread, NULL, heart_beat, NULL)) {
		fprintf(stderr, "Error creating heartbeat thread\n");
		return false;
	}
	

	log_write("Server initialized\n");
	return true;

cleanup:
	cleanup();
	return false;
}


// Hash iterator for freeing memory used by values; called during storage cleanup
static void clean_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	log_write("\t\tK: %s,\n\t\t\t V:%s \n", key_to_str(key), value);
	(void)key;
	(void)value_sz;
	(void)arg;

	assert(value != NULL);
	free(value);
}

// Cleanup and release all the resources
static void cleanup()
{
	heart_stopped = 1;
	log_write("\tmserver_fd_out = %d \n", mserver_fd_out);
	log_write("\tmserver_fd_in = %d \n", mserver_fd_in);
	log_write("\tmy_clients_fd = %d \n", my_clients_fd);
	log_write("\tmy_servers_fd = %d \n", my_servers_fd);
	log_write("\tmy_mservers_fd = %d \n", my_mservers_fd);

	log_write("\tSecondary server (the one that stores the secondary copy for this server's primary key set)\n");
	log_write("\t\tsecondary_fd = %d \n", secondary_fd);
	log_write("\t\tsecondary_sid = %d \n", secondary_sid);

	log_write("\tPrimary server (the one that stores the primary copy for this server's secondary key set)\n");
	log_write("\t\tprimary_fd = %d \n", primary_fd);
	log_write("\t\tprimary_sid = %d \n", primary_sid);



	close_safe(&mserver_fd_out);
	close_safe(&mserver_fd_in);
	close_safe(&my_clients_fd);
	close_safe(&my_servers_fd);
	close_safe(&my_mservers_fd);
	close_safe(&secondary_fd);
	close_safe(&primary_fd);

	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
		close_safe(&(client_fd_table[i]));
	}
	for (int i = 0; i < 2; i++) {
		close_safe(&(server_fd_table[i]));
	}


	log_write("\tPrimary Hash { \n");
	hash_iterate(&primary_hash, clean_iterator_f, NULL);
	hash_cleanup(&primary_hash);
	

	log_write("\t}\n\tSecondary Hash { \n");
	hash_iterate(&secondary_hash, clean_iterator_f, NULL);
	hash_cleanup(&secondary_hash);
	log_write("\t}\n");



	pthread_join(heart_beat_thread, NULL);

	pthread_join(primary_update_thread, NULL);

	pthread_join(secondary_update_thread, NULL);
}











//----------------------------------------
//------	CLIENT MESSAGES		----------
//----------------------------------------

// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
	log_write("%s Receiving a client message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		return;
	}
	operation_request *request = (operation_request*)req_buffer;

	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response*)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;
	uint16_t value_sz = 0;



	int key_srv_id = key_server_id(request->key, num_servers);

	//if key does not belong in this server, check if it belongs in its primary server
	if(key_srv_id == primary_sid){
		bool is_key_found = false;
		log_write("\tRedirecting Clients Operations to my secondary Hash\n");

		void *data = NULL;
		size_t size = 0;

		if (hash_get(&secondary_hash, request->key, &data, &size)) {
			// Found a key in the secondary hash
			hash_lock(&secondary_hash, request->key);
			is_key_found = true;
		}
		switch (request->type) {
			case OP_NOOP:{
				response->status = SUCCESS;
				break;
			}
			case OP_GET: {
				if(!is_key_found){
					log_write("Key %s not found\n", key_to_str(request->key));
					response->status = KEY_NOT_FOUND;
					//break;
				}
				else{
					memcpy(response->value, data, size);
					value_sz = size;
					response->status = SUCCESS;
				}
				break;
			}
			case OP_PUT: {

				size_t value_size = request->hdr.length - sizeof(*request);
				void *value_copy = malloc(value_size);
				if (value_copy == NULL) {
					perror("malloc");
					fprintf(stderr, "sid %d: Out of memory\n", server_id);
					response->status = OUT_OF_SPACE;
					break;
				}

				memcpy(value_copy, request->value, value_size);
				void *old_value = NULL;
				size_t old_value_sz = 0;

				if (!hash_put(&secondary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
				{
					fprintf(stderr, "sid %d: Out of memory\n", server_id);
					free(value_copy);
					response->status = OUT_OF_SPACE;
					break;
				}
				char recv_buffer[MAX_MSG_LEN] = {0};
				//Forward Message to Primary server
				if (!send_msg(primary_fd, request, sizeof(*request) + value_size) ||
	    			!recv_msg(primary_fd, recv_buffer, sizeof(recv_buffer), MSG_OPERATION_RESP)){

					fprintf(stderr, "sid %d: Failed Secondary Server Put\n", server_id);
					response->status = SERVER_FAILURE;
					break;
				}

				//get the response from the secondary server
				operation_response *response_secondary = (operation_response*)recv_buffer;			

				//check if the secondary server doesn't fail, not entirely sure if necessary,
				if(response_secondary->status != SUCCESS){
					response->status = SERVER_FAILURE;
					break;
				}
	
				// Need to free the old value (if there was any)
				if (old_value != NULL) {
					free(old_value);
				}

				response->status = SUCCESS;
				break;	
				//Send message to Primary server
				//Recieve message from Primary server

			}
			default:{
				fprintf(stderr, "sid %d: Invalid client operation type\n", server_id);
				return;
			}

		}
		if(is_key_found){
			hash_unlock(&secondary_hash, request->key);
		}
		send_msg(fd, response, sizeof(*response) + value_sz);
		return;
	}
	else if(key_srv_id == secondary_sid && !switched_to_primary)
		log_write("\tFORWARD TO PRIMARY %d\n", primary_sid);
	
//	*/

	//-------------------PRIMARY---------------
	// Check that requested key is valid
	//int key_srv_id = key_server_id(request->key, num_servers);
	if (key_srv_id != server_id) {
		fprintf(stderr, "sid %d: Invalid client key %s sid %d\n", server_id, key_to_str(request->key), key_srv_id);
		response->status = KEY_NOT_FOUND;
		send_msg(fd, response, sizeof(*response) + value_sz);
		return;
	}

	// Process the request based on its type
	switch (request->type) {
		case OP_NOOP:
			response->status = SUCCESS;
			break;

		case OP_GET: {
			void *data = NULL;
			size_t size = 0;

			// Get the value for requested key from the hash table
			if (!hash_get(&primary_hash, request->key, &data, &size)) {
				log_write("Key %s not found\n", key_to_str(request->key));
				response->status = KEY_NOT_FOUND;
				break;
			}

			// Copy the stored value into the response buffer
			memcpy(response->value, data, size);
			value_sz = size;

			response->status = SUCCESS;
			break;
		}

		case OP_PUT: {
			// Need to copy the value to dynamically allocated memory
			size_t value_size = request->hdr.length - sizeof(*request);
			void *value_copy = malloc(value_size);
			if (value_copy == NULL) {
				perror("malloc");
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				response->status = OUT_OF_SPACE;
				break;
			}
			memcpy(value_copy, request->value, value_size);

			void *old_value = NULL;
			size_t old_value_sz = 0;

			// Put the <key, value> pair into the hash table
			if (!hash_put(&primary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz))
			{
				fprintf(stderr, "sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}

			log_write("\tServer forwarding PUT request to secondary hash\n");
			char recv_buffer[MAX_MSG_LEN] = {0};
			//Forward the put request to the secondary server and check if
			// that server sends a Success Message 
			if (!send_msg(secondary_fd, request, sizeof(*request) + value_size) ||
	    		!recv_msg(secondary_fd, recv_buffer, sizeof(recv_buffer), MSG_OPERATION_RESP))
			{
				fprintf(stderr, "sid %d: Failed Secondary Server Put\n", server_id);
				response->status = SERVER_FAILURE;
				break;
			}

			// get the response from the secondary server
			operation_response *response_secondary = (operation_response*)recv_buffer;			

			// check that the response from the if the secondary server doesn't fail
			if(response_secondary->status != SUCCESS){
				response->status = SERVER_FAILURE;
				break;
			}
	

			// Need to free the old value (if there was any)
			if (old_value != NULL) {
				free(old_value);
			}

			response->status = SUCCESS;
			break;
		}

		default:
			fprintf(stderr, "sid %d: Invalid client operation type\n", server_id);
			return;
	}

	// Send reply to the client
	send_msg(fd, response, sizeof(*response) + value_sz);
}










//----------------------------------------
//------	SERVER MESSAGES		----------
//----------------------------------------

// Returns false if either the message was invalid or if this was the last message
// (in both cases the connection will be closed)
static bool process_server_message(int fd)
{
	log_write("%s Receiving a server message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ)) {
		return false;
	}
	operation_request *request = (operation_request*)req_buffer;

	// NOOP operation request is used to indicate the last message in an UPDATE sequence
	if (request->type == OP_NOOP) {
		log_write("Received the last server message, closing connection\n");
		return false;
	}

	// TODO: process the message and send the response
	// ...

	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response*)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;


	if (request->type == OP_PUT) {

		//get value from request 
		size_t value_size = request->hdr.length - sizeof(*request);
		void *value_copy = malloc(value_size);

		// Make sure there is enough memory for copying
		if (value_copy == NULL) {
			perror("malloc");
			fprintf(stderr, "sid %d: Out of memory\n", server_id);
			response->status = OUT_OF_SPACE;
			send_msg(fd, response, sizeof(*response) + value_size);
			return false;
		}

		memcpy(value_copy, request->value, value_size);
		void *old_value = NULL;
		size_t old_value_sz = 0;
		int key_srv_id = key_server_id(request->key, num_servers);

		log_write("\tServer reciecving: Putting in ");

		// if the server that the key is from is from the Primary server
		// then put the key-value pairing in this server secondary set
		if(key_srv_id == primary_sid){
			log_write("my Secondary Hash, message from Primary Server \n");
			primary_fd = fd;
			//put (key,value) into secondary array
			if (!hash_put(&secondary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz)){
					fprintf(stderr, "sid %d: Out of memory\n", server_id);
					free(value_copy);
					response->status = OUT_OF_SPACE;

					//send response and return false
					send_msg(fd, response, sizeof(*response) + value_size);
					return false;
			}
		}
		// Else the key is from the secondary server, and we then put
		// the key-value pairing in this server secondary set
		else{
			log_write("my Primary Hash, message from Secondary Server \n");
			//put (key,value) into secondary array
			//secondary_fd = fd;
			if (!hash_put(&primary_hash, request->key, value_copy, value_size, &old_value, &old_value_sz)){
					fprintf(stderr, "sid %d: Out of memory\n", server_id);
					free(value_copy);
					response->status = OUT_OF_SPACE;
					//send response and return false
					send_msg(fd, response, sizeof(*response) + value_size);
					return false;
			}
		}
		
		response->status = SUCCESS;
		//send response
		send_msg(fd, response, sizeof(*response) + value_size);
	}

	return true;
}









//----------------------------------------
//-------	METADATA MESSAGES	----------
//----------------------------------------

// Returns false if the message was invalid (so the connection will be closed)
// Sets *shutdown_requested to true if received a SHUTDOWN message (so the server will terminate)
static bool process_mserver_message(int fd, bool *shutdown_requested)
{
	assert(shutdown_requested != NULL);
	*shutdown_requested = false;

	log_write("%s Receiving a metadata server message\n", current_time_str());

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_SERVER_CTRL_REQ)) {
		return false;
	}
	server_ctrl_request *request = (server_ctrl_request*)req_buffer;

	// Initialize the response
	server_ctrl_response response = {0};
	response.hdr.type = MSG_SERVER_CTRL_RESP;

	// Process the request based on its type
	switch (request->type) {
		case SET_SECONDARY:
			response.status = ((secondary_fd = connect_to_server(request->host_name, request->port)) < 0)
			                ? CTRLREQ_FAILURE : CTRLREQ_SUCCESS;
			break;

		case SHUTDOWN:
			*shutdown_requested = true;
			return true;

		// TODO: handle remaining message types
		case UPDATE_PRIMARY:

			//set this server to handle primary requests for the recovering server 
			switched_to_primary = true;

			// Connect this servers primary file descriptor to the request's port
			primary_fd = connect_to_server(request->host_name, request->port);

			//	Spawn a thread to update the Secondary Server by sending this server's 
			//	primary key-values to it 
			if(pthread_create(&primary_update_thread, NULL, send_primary_hash, NULL)) {
				fprintf(stderr, "Error creating primary update thread\n");
				response.status = CTRLREQ_FAILURE;
				break;	
			}
			response.status = CTRLREQ_SUCCESS;
			break;

		case UPDATE_SECONDARY:

			// Connect this servers secondary file descriptor to the request's port
			secondary_fd = connect_to_server(request->host_name, request->port);

			//	Spawn a thread to update the Secondary Server by sending this server's 
			//	primary key-values to it 
			if(pthread_create(&secondary_update_thread, NULL, send_secondary_hash, NULL)) {
				fprintf(stderr, "Error creating secondary update thread\n");
				response.status = CTRLREQ_FAILURE;
				break;	
			}
			
			response.status = CTRLREQ_SUCCESS;
			break;
		
		case SWITCH_PRIMARY:
			switched_to_primary = false;
			response.status = CTRLREQ_SUCCESS;
			break;

		default:// impossible
			assert(false);
			break;
	}

	send_msg(fd, &response, sizeof(response));
	return true;
}


// Returns false if stopped due to errors, true if shutdown was requested
static bool run_server_loop()
{
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_clients_fd, &allset);
	FD_SET(my_servers_fd, &allset);
	FD_SET(my_mservers_fd, &allset);

	int maxfd = max(my_clients_fd, my_servers_fd);
	maxfd = max(maxfd, my_mservers_fd);

	// Server sits in an infinite loop waiting for incoming connections from mserver/servers/client
	// and for incoming messages from already connected mserver/servers/clients
	for (;;) {
		rset = allset;

		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
		if (num_ready_fds < 0) {
			perror("select");
			return false;
		}

		if (num_ready_fds <= 0) {
			continue;
		}

		// Incoming connection from the metadata server
		if (FD_ISSET(my_mservers_fd, &rset)) {
			int fd_idx = accept_connection(my_mservers_fd, &mserver_fd_in, 1);
			if (fd_idx >= 0) {
				FD_SET(mserver_fd_in, &allset);
				maxfd = max(maxfd, mserver_fd_in);
			}
			assert(fd_idx == 0);

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Incoming connection from a key-value server
		if (FD_ISSET(my_servers_fd, &rset)) {
			int fd_idx = accept_connection(my_servers_fd, server_fd_table, 2);
			if (fd_idx >= 0) {
				FD_SET(server_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, server_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Incoming connection from a client
		if (FD_ISSET(my_clients_fd, &rset)) {
			int fd_idx = accept_connection(my_clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
			if (fd_idx >= 0) {
				FD_SET(client_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, client_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from the metadata server
		if ((mserver_fd_in != -1) && FD_ISSET(mserver_fd_in, &rset)) {
			bool shutdown_requested = false;
			if (!process_mserver_message(mserver_fd_in, &shutdown_requested)) {
				// Received an invalid message, close the connection
				FD_CLR(mserver_fd_in, &allset);
				close_safe(&(mserver_fd_in));
			} else if (shutdown_requested) {
				return true;
			}

			if (--num_ready_fds <= 0) {
				continue;
			}
		}

		// Check for any messages from connected key-value servers
		for (int i = 0; i < 2; i++) {
			if ((server_fd_table[i] != -1) && FD_ISSET(server_fd_table[i], &rset)) {
				if (!process_server_message(server_fd_table[i])) {
					// Received an invalid message (or the last valid message), close the connection
					FD_CLR(server_fd_table[i], &allset);
					close_safe(&(server_fd_table[i]));
				}

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
		if (num_ready_fds <= 0) {
			continue;
		}

		// Check for any messages from connected clients
		for (int i = 0; i < MAX_CLIENT_SESSIONS; i++) {
			if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset)) {
				process_client_message(client_fd_table[i]);
				// Close connection after processing (semantics are "one connection per request")
				FD_CLR(client_fd_table[i], &allset);
				close_safe(&(client_fd_table[i]));

				if (--num_ready_fds <= 0) {
					break;
				}
			}
		}
	}
}


int main(int argc, char **argv)
{
	if (!parse_args(argc, argv)) {
		usage(argv);
		return 1;
	}

	open_log(log_file_name);

	if (!init_server()) {
		return 1;
	}

	run_server_loop();

	cleanup();
	return 0;
}
