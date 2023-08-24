#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include <stdio.h>

#define DATAS ("testdb")

// Initialization of mutexes used
pthread_mutex_t writeStatsToFile = PTHREAD_MUTEX_INITIALIZER;	// For printing stats to file
pthread_mutex_t dbLock = PTHREAD_MUTEX_INITIALIZER;				// To add values to the db
pthread_mutex_t dbOpenClose = PTHREAD_MUTEX_INITIALIZER;		// To open and close the db
pthread_mutex_t writeMutex = PTHREAD_MUTEX_INITIALIZER;			// For the cond variable

pthread_cond_t writeDone = PTHREAD_COND_INITIALIZER;			// (for readwrite function)


FILE *fp;														// For printing stats, to an output.txt file
											
DB* db;															// Changed from local variable to global, so all threads have access to the same db
int threadCounter = 0;											// Counter, used for the opening and closing of the database
int writeFlag = 0; 												// Used as a counter, to check if writing has been completed

void * _write_test(void *arg) 									// Changed from "_write_test(long int count, int r)", so we can pass multiple arguments to threads
{
	pthread_mutex_lock(&writeMutex);							// Each thread increases the writeFlag (reading is disabled till writeFlag==0)
	writeFlag++;												//
	pthread_mutex_unlock(&writeMutex);							// Continue execution.

	struct data *d = (struct data *) arg; 						// Casting of *arg to struct data
	long int count = d->count; 									// Initializing count, from arguments received
	int r = d->r; 												// Initializing r, from arguments received
	int i;

	double cost;
	long long start,end;
	Variant sk, sv;
	//DB* db;													// Changed to global

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	pthread_mutex_lock(&dbOpenClose);							// Each thread
	threadCounter++;											// increases the threadCounter by one
	if (threadCounter == 1){									// If it's the 1st thread
		db = db_open(DATAS);									// it opens the database
	}															//
	pthread_mutex_unlock(&dbOpenClose);							// and continues

	start = get_ustime_sec();
		for (i = d->index_start; i < d->index_end; i++) {		// Changed so each thread reads different section					

		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;
		
		pthread_mutex_lock(&dbLock);							// Each thread locks the database 
		db_add(db, &sk, &sv);									// to add a new key 
		pthread_mutex_unlock(&dbLock);							// then unlocks

		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		
		}
	}

	pthread_mutex_lock(&dbOpenClose);							// Each thread 
	threadCounter--;											// decreases the threadCounter
	if (threadCounter == 0) {									// and if it's the last thread
		db_close(db);											// it closes the database
	}															//
	pthread_mutex_unlock(&dbOpenClose);							// and continues


	// For the readwrite function
	pthread_mutex_lock(&writeMutex);							// Each thread
	writeFlag--;												// decreases the writeFlag
	if (writeFlag == 0) {										// and if writeFlag reaches zero 
		pthread_cond_broadcast(&writeDone);						// Signal to all threads waiting (to read) to start execution
	}															//
	pthread_mutex_unlock(&writeMutex);							// then unlocks


	end = get_ustime_sec();
	cost = end -start;

	// Writing Random-Write stats to a file
	pthread_mutex_lock(&writeStatsToFile); 
	fp = fopen("output.txt","a+");
	long int threadTotal = d->index_end-d->index_start;	
	fprintf(fp, "Thread %ld completed execution, %ld writes: %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,d->tid, threadTotal, (double) (cost/threadTotal), (double) (threadTotal/cost), cost);
	fclose(fp);
	pthread_mutex_unlock(&writeStatsToFile); 
	//

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void * _read_test(void *arg) 									// Changed from "_read_test(long int count, int r)", so we can pass multiple arguments to threads
{
	pthread_mutex_lock(&writeMutex);							// For readwrite, each reading thread
	while (writeFlag > 0) {										// if there's writing in progress
		pthread_cond_wait(&writeDone, &writeMutex);				// wait for a signal/broadcast to continue
	}															//
	pthread_mutex_unlock(&writeMutex);							// then unlock

	struct data *d = (struct data *) arg; 						// Casting of *arg to struct data
	long int count = d->count; 									// Initializing count, from arguments received
	int r = d->r; 												// Initializing r, from arguments received
					
	int i;
	int ret;								
	int found = 0;
	double cost;
	long long start,end;
	Variant sk;
	Variant sv;
	//DB* db;													// Changed to global
	char key[KSIZE + 1];
	
	pthread_mutex_lock(&dbOpenClose);							// Each thread
	threadCounter++;											// increases the threadCounter by one
	if (threadCounter == 1){									// If it's the 1st thread
		db = db_open(DATAS);									// it opens the database
	}															//
	pthread_mutex_unlock(&dbOpenClose);							// and continues

	start = get_ustime_sec();
	
	for (i = d->index_start; i < d->index_end; i++) {			// Changed so each thread reads different section
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d searching %s\n", i, key);
		sk.length = KSIZE;
		sk.mem = key;
		
		ret = db_get(db, &sk, &sv);			
		if (ret) {
			//db_free_data(sv.mem);
			found++;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}	

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}		
	}
	
	pthread_mutex_lock(&dbOpenClose);							// Each thread 
	threadCounter--;											// decreases the threadCounter
	if (threadCounter == 0) {									// and if it's the last thread
		db_close(db);											// it closes the database
	}															//
	pthread_mutex_unlock(&dbOpenClose);							// and continues
	
	
	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	
	// Writing Random-Read stats to a file, threads write to file one by one
	pthread_mutex_lock(&writeStatsToFile); 
	fp = fopen("output.txt","a+");
	long int threadTotal = d->index_end-d->index_start;	
	fprintf(fp, "Thread %ld completed execution, found %d/%ld reads: %.6f sec/op; %.1f reads/sec(estimated); cost:%.3f(sec);\n"
		,d->tid, found, threadTotal, (double) (cost/threadTotal), (double) (threadTotal/cost), cost);
	fclose(fp);
	pthread_mutex_unlock(&writeStatsToFile); 
	//

	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);									
}
