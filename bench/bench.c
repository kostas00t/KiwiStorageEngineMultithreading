#include "bench.h"

void _random_key(char *key,int length) {
	int i;
	char salt[36]= "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n", 
			KSIZE);
	printf("Values: \t%d bytes each\n", 
			VSIZE);
	printf("Entries:\t%d\n", 
			count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
			index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
			data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s", 
			(char*)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo) {
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL) {
			const char* sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep-1-line);
			strncpy(val, sep+1, strlen(sep)-1);
			if (strcmp("model name", key) == 0) {
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);	
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s", 
				num_cpus, 
				cpu_type);

		printf("CPUCache:\t%s\n", 
				cache_size);
	}
}

int main(int argc,char** argv)
{
	long int count;

	srand(time(NULL));
	if (argc < 3) {
		fprintf(stderr,"Usage: db-bench (<write | read> <count> <random>) (<readwrite> <count> <writePercentage> <random>) \n");  // Changed to accept more parameters
		exit(1);
	}
	
	if (strcmp(argv[1], "write") == 0) {
		int r = 0;

		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		if (argc == 4)
			r = 1;

		struct data thread_args[numOfThreads]; 																// Initializing thread_args array, a struct for the parameters we want to pass to _write_test, for each thread
		long int index_n = count/numOfThreads;																// The workload for each thread, the total amount of writes divided by the number of threads
		int remainder = count % numOfThreads;																// Covering the case of count not int divisible by numOfThreads 
		long int index_start = 0;																			// Start of writing, changes for each thread								
		long int index_end = index_start + index_n;															// End of writing, changes for each thread
		for (int i = 0; i < numOfThreads; i++) {															//
			thread_args[i].count = count; 																	// Passing count into thread_args[i]
			thread_args[i].r = r; 																			// Passing r into thread_args[i]
			thread_args[i].index_start = index_start;														// Passing index_start into thread_args[i]
			thread_args[i].index_end = index_end;															// Passing index_end into thread_args[i]	
			pthread_create(&thread_args[i].tid, NULL, _write_test, (void *) &thread_args[i]);				// Creating a new thread, calls _write_test function, with the parameters of thread_args 
			index_start = index_end;																		// Change index_start for the next thread
			if (i == numOfThreads-2) {																		// If the next thread, is the last thread
				index_end = index_end + index_n + remainder;												// Change index_end to index_end + index_n + remainder 
			} else {																						// If it's not
				index_end = index_end + index_n;															// Change index_end to index_end + index_n
			}																								//
		}																									//
		for (int i = 0; i < numOfThreads; i++){																// 
			pthread_join(thread_args[i].tid, NULL);															// Blocks execution till each thread has completed it's task
		}																									//		
			
		//_write_test(count, r);																			// Initial code
	} else if (strcmp(argv[1], "read") == 0) {
		int r = 0;

		count = atoi(argv[2]);
		_print_header(count);
		_print_environment();
		if (argc == 4)
			r = 1;
		
		struct data thread_args[numOfThreads]; 																// Initializing thread_args array, a struct for the parameters we want to pass to _read_test, for each thread
		long int index_n = count/numOfThreads;																// The workload for each thread, the total amount of reads divided by the number of threads
		int remainder = count % numOfThreads;																// Covering the case of count not int divisible by numOfThreads 
		long int index_start = 0;																			// Start of reading, changes for each thread								
		long int index_end = index_start + index_n;															// End of reading, changes for each thread
		for (int i = 0; i < numOfThreads; i++) {															//
			thread_args[i].count = count; 																	// Passing count into thread_args[i]
			thread_args[i].r = r; 																			// Passing r into thread_args[i]
			thread_args[i].index_start = index_start;														// Passing index_start into thread_args[i]
			thread_args[i].index_end = index_end;															// Passing index_end into thread_args[i]
			pthread_create(&thread_args[i].tid, NULL, _read_test, (void *) &thread_args[i]);				// Creating a new thread, calls _read_test function, with the parameters of thread_args 
			index_start = index_end;																		// Change index_start for the next thread
			if (i == numOfThreads-2) {																		// If the next thread, is the last thread
				index_end = index_end + index_n + remainder;												// Change index_end to index_end + index_n + remainder 
			} else {																						// If it's not
				index_end = index_end + index_n;															// Change index_end to index_end + index_n
			}																								//
		}																									//
		for (int i = 0; i < numOfThreads; i++){																// 
			pthread_join(thread_args[i].tid, NULL);															// Blocks execution till each thread has completed it's task
		}																									//

		//_read_test(count, r);																				// Initial code

	} else if (strcmp(argv[1], "readwrite") == 0) {															// New case, for reading and writing
		int r = 0;																							// -Same as above-
		count = atoi(argv[2]);																				// 
		_print_header(count);																				//
		_print_environment();																				//
		if (argc == 5)																						//
			r = 1;																							// -Same as above-
		int writePercentage = atoi(argv[3]);																// Save the write percentage
		if (writePercentage < 1 || writePercentage > 99) {													// Check for invalid write percentage 
			printf("Write percentage should be in range [1-99]\n");											//
			exit(-1);																						//
		}																									//
		int readPercentage = 100-writePercentage;															// Calculate the read percentage



		struct data thread_args_write[writePercentage]; 													// Initializing thread_args array, a struct for the parameters we want to pass to _write_test, for each thread
		long int index_n_write = ((count*writePercentage)/100)/writePercentage;								// The workload for each thread, the total amount of writes divided by the number of threads
		int remainder_write = ((count*writePercentage)/100) % writePercentage;								// Covering the case of count not int divisible by numOfThreads 
		long int index_start_write = 0;																		// Start of writing, changes for each thread								
		long int index_end_write = index_start_write + index_n_write;										// End of writing, changes for each thread
		for (int i = 0; i < writePercentage; i++) {															//
			thread_args_write[i].count = ((count*writePercentage)/100); 									// Passing count into thread_args[i]
			thread_args_write[i].r = r; 																	// Passing r into thread_args[i]
			thread_args_write[i].index_start = index_start_write;											// Passing index_start into thread_args[i]
			thread_args_write[i].index_end = index_end_write;												// Passing index_end into thread_args[i]		
			pthread_create(&thread_args_write[i].tid, NULL, _write_test, (void *) &thread_args_write[i]);	// Creating a new thread, calls _write_test function, with the parameters of thread_args 
			index_start_write = index_end_write;															// Change index_start for the next thread
			if (i == writePercentage-2) {																	// If the next thread, is the last thread
				index_end_write = index_end_write + index_n_write + remainder_write;						// Change index_end to index_end + index_n + remainder 
			} else {																						// If it's not
				index_end_write = index_end_write + index_n_write;											// Change index_end to index_end + index_n
			}																								//
		}																									//


		struct data thread_args_read[readPercentage]; 														// Initializing thread_args array, a struct for the parameters we want to pass to _read_test, for each thread
		long int index_n_read = ((count*readPercentage)/100)/readPercentage;								// The workload for each thread, the total amount of reads divided by the number of threads
		int remainder_read = ((count*readPercentage)/100) % readPercentage;									// Covering the case of count not int divisible by numOfThreads 
		long int index_start_read = 0;																		// Start of reading, changes for each thread								
		long int index_end_read = index_start_read + index_n_read;											// End of reading, changes for each thread
		for (int i = 0; i < readPercentage; i++) {															//
			thread_args_read[i].count = ((count*readPercentage)/100); 										// Passing count into thread_args[i]
			thread_args_read[i].r = r; 																		// Passing r into thread_args[i]
			thread_args_read[i].index_start = index_start_read;												// Passing index_start into thread_args[i]
			thread_args_read[i].index_end = index_end_read;													// Passing index_end into thread_args[i]
			pthread_create(&thread_args_read[i].tid, NULL, _read_test, (void *) &thread_args_read[i]);		// Creating a new thread, calls _read_test function, with the parameters of thread_args 
			index_start_read = index_end_read;																// Change index_start for the next thread
			if (i == readPercentage-2) {																	// If the next thread, is the last thread
				index_end_read = index_end_read + index_n_read + remainder_read;							// Change index_end to index_end + index_n + remainder 
			} else {																						// If it's not
				index_end_read = index_end_read + index_n_read;												// Change index_end to index_end + index_n
			}																								//
		}																									//
																											//		
		for (int i = 0; i < writePercentage; i++){															// 
			pthread_join(thread_args_write[i].tid, NULL);													// Blocks execution till each thread has completed it's task
		}																									//		
		for (int i = 0; i < readPercentage; i++){															// 
			pthread_join(thread_args_read[i].tid, NULL);													// Blocks execution till each thread has completed it's task
		}																									//

	} else {
		fprintf(stderr,"Usage: db-bench (<write | read> <count> <random>) (<readwrite> <count> <writePercentage> <random>) \n"); // Changed to accept more parameters
		exit(1);
	}

	return 1;
}
