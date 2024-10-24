//**********************************************************************************
// Jonathan Cunningham - CMPSC472 FW24 (10/23/24)
// PROJECT #1 ~ File processing system with multiprocessing and multithreading
//
// DESCRIPTION:
// This program processes multiple large text files in parallel by using 
// multiprocessing and multithreading. It first searches for a user-specified word 
// in each file and counts its occurrences. Each file is handled by a separate 
// child process created using fork(), and within each process, multiple threads 
// are spawned to split the file into chunks and count the occurrences of the word 
// in parallel. The child processes communicate the word counts back to the 
// parent process using pipes. Finally, the parent process aggregates the results 
// and displays the total count of the word across all files.
//**********************************************************************************

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/time.h>

#define NUM_FILES 7
#define NUM_THREADS 4
#define BUFFER_SIZE 1024

// Structure to pass thread data
typedef struct {
    const char *filename;
    long start;
    long end;
    const char *word_to_count;  // Word to be searched in the thread
} ThreadData;

pthread_mutex_t mutex;
int word_count = 0;

// Function to count word occurrences in a buffer
int count_word_in_buffer(const char *buffer, const char *word) {
    int count = 0;
    const char *tmp = buffer;
    while ((tmp = strstr(tmp, word)) != NULL) {
        count++;
        tmp += strlen(word);
    }
    return count;
}

// Thread function to count words in a file chunk
void* count_words(void *arg) {
    ThreadData *data = (ThreadData*)arg;

    FILE *file = fopen(data->filename, "r");
    if (!file) {
        perror("Thread file open failed");
        return NULL;
    }

    fseek(file, data->start, SEEK_SET);

    char buffer[BUFFER_SIZE];
    int local_count = 0;

    // Reading in the range of the chunk assigned to this thread
    while (ftell(file) < data->end && fgets(buffer, BUFFER_SIZE, file) != NULL) {
        local_count += count_word_in_buffer(buffer, data->word_to_count);
    }

    fclose(file);

    // Update the global word count with thread-specific count
    pthread_mutex_lock(&mutex);
    word_count += local_count;
    pthread_mutex_unlock(&mutex);

    return NULL;
}

// Function to create threads and process a file
void process_file(const char *filename, const char *word_to_count) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("File open failed");
        exit(EXIT_FAILURE);
    }

    fseek(file, 0L, SEEK_END);
    long file_size = ftell(file);
    fclose(file);  // Close after getting file size, each thread opens its own file

    long chunk_size = file_size / NUM_THREADS;

    pthread_t threads[NUM_THREADS];
    ThreadData thread_data[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].filename = filename;
        thread_data[i].start = i * chunk_size;
        thread_data[i].end = (i == NUM_THREADS - 1) ? file_size : (i + 1) * chunk_size;
        thread_data[i].word_to_count = word_to_count;
        pthread_create(&threads[i], NULL, count_words, &thread_data[i]);
    }

    // Wait for all threads to finish
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
}

// Function to print combined CPU usage (in microseconds)
void print_resource_usage() {
    struct rusage usage;

    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        long total_cpu_time = 
            (usage.ru_utime.tv_sec * 1000000 + usage.ru_utime.tv_usec) + 
            (usage.ru_stime.tv_sec * 1000000 + usage.ru_stime.tv_usec);
        printf("Total CPU time taken: %ld microseconds\n", total_cpu_time);
        printf("Maximum memory usage: %ld kilobytes\n", usage.ru_maxrss);  // Memory usage
    } else {
        perror("getrusage failed");
    }
}

int main() {
    const char *files[NUM_FILES] = {
        "bib.txt", "paper1.txt", "paper2.txt",
        "progc.txt", "progl.txt", "progp.txt", "trans.txt"
    };

    char word_to_count[100];
    printf("FILE PROCESSING WORD COUNTER W/ MULTIPROCESSING AND MULTITHREADING\n");
    printf("*******************************************************************\n");
    printf("Please enter the word to count: ");
    scanf("%s", word_to_count);
    printf("\n");

    // Start timing
    struct timeval start, end;
    gettimeofday(&start, NULL);

    int pipes[NUM_FILES][2];
    pid_t pids[NUM_FILES];
    pthread_mutex_init(&mutex, NULL);

    // Create pipes and fork child processes
    for (int i = 0; i < NUM_FILES; i++) {
        if (pipe(pipes[i]) == -1) {
            perror("pipe");
            exit(EXIT_FAILURE);
        }

        pids[i] = fork();

        if (pids[i] == -1) {
            perror("fork");
            exit(EXIT_FAILURE);
        }

        if (pids[i] == 0) {  // Child process
            close(pipes[i][0]);  // Close reading end

            word_count = 0;  // Reset word count
            process_file(files[i], word_to_count);  // Process the file with user input word

            // Write result to pipe
            write(pipes[i][1], &word_count, sizeof(int));
            close(pipes[i][1]);  // Close writing end

            exit(EXIT_SUCCESS);
        } else {  // Parent process
            close(pipes[i][1]);  // Close writing end
        }
    }

    // Parent process: Read results from pipes
    int total_word_count = 0;
    for (int i = 0; i < NUM_FILES; i++) {
        int file_word_count = 0;
        read(pipes[i][0], &file_word_count, sizeof(int));
        close(pipes[i][0]);  // Close reading end
        total_word_count += file_word_count;

        printf("File %s: %d occurrences of the word '%s'\n", files[i], file_word_count, word_to_count);
    }

    // Wait for all child processes to finish
    for (int i = 0; i < NUM_FILES; i++) {
        waitpid(pids[i], NULL, 0);
    }

    // End timing
    gettimeofday(&end, NULL);
    long elapsed_time = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec);
    printf("------------------------------------------------------------------\n");
    printf("Total time taken: %ld microseconds\n", elapsed_time);

    // Print CPU and memory usage
    print_resource_usage();

    printf("------------------------------------------------------------------\n");
    printf("TOTAL OCCURRENCES OF '%s' ACROSS ALL FILES: %d\n", word_to_count, total_word_count);
    pthread_mutex_destroy(&mutex);

    return 0;
}
