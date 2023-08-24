#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>		// Added to use pthread 

#define KSIZE (16)
#define VSIZE (1000)

#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"

#define numOfThreads 16 					// Number of available threads

long long get_ustime_sec(void);
void _random_key(char *key,int length);
void * _read_test(void *arg); 			// Changed from "_read_test(long int count, int r)", so we can pass multiple arguments to threads
void * _write_test(void *arg); 			// Changed from "_write_test(long int count, int r)", so we can pass multiple arguments to threads

struct data { 							// Definition of struct data
	long int count; 					//
	int r; 								//
	long int index_start, index_end;	//
    pthread_t     tid;					//
}; 										//
