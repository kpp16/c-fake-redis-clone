#include <criterion/criterion.h>
#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <wait.h>

static void init() {
#ifndef NO_SERVER
    int ret;
    int i = 0;
    do { // Wait for server to start
	ret = system("netstat -an | fgrep '0.0.0.0:9999' > /dev/null");
	sleep(1);
    } while(++i < 30 && WEXITSTATUS(ret));
#endif
}

static void fini() {
}

/*
 * Thread to run a command using system() and collect the exit status.
 */
void *system_thread(void *arg) {
    long ret = system((char *)arg);
    return (void *)ret;
}

int ret1, ret2;

void* input_1(void* x) {
    ret1 = system("util/client -p 9999 -q < tests/input02connect > tests/output02connect");
    return x;
    // cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

void* input_2(void* x) {
    ret2 = system("util/client -p 9999 -q < tests/input03connect > tests/output03connect");
    return x;  
    // cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

// Criterion seems to sort tests by name.  This one can't be delayed
// or others will time out.
Test(student_suite, 00_start_server, .timeout = 30) {
    fprintf(stderr, "server_suite/00_start_server\n");
    int server_pid = 0;
    int ret = system("netstat -an | fgrep '0.0.0.0:9999' > /dev/null");
    cr_assert_neq(WEXITSTATUS(ret), 0, "Server was already running");
    fprintf(stderr, "Starting server...");
    if((server_pid = fork()) == 0) {
	execlp("valgrind", "xacto", "--leak-check=full", "--track-fds=yes",
	       "--error-exitcode=37", "--log-file=valgrind.out", "bin/xacto", "-p", "9999", NULL);
	fprintf(stderr, "Failed to exec server\n");
	abort();
    }
    fprintf(stderr, "pid = %d\n", server_pid);
    char *cmd = "sleep 15";
    pthread_t tid;
    pthread_create(&tid, NULL, system_thread, cmd);
    pthread_join(tid, NULL);
    cr_assert_neq(server_pid, 0, "Server was not started by this test");
    fprintf(stderr, "Sending SIGHUP to server pid %d\n", server_pid);
    kill(server_pid, SIGHUP);
    sleep(5);
    kill(server_pid, SIGKILL);
    wait(&ret);
    fprintf(stderr, "Server wait() returned = 0x%x\n", ret);
    if(WIFSIGNALED(ret)) {
	fprintf(stderr, "Server terminated with signal %d\n", WTERMSIG(ret));	
	system("cat valgrind.out");
	if(WTERMSIG(ret) == 9)
	    cr_assert_fail("Server did not terminate after SIGHUP");
    }
    if(WEXITSTATUS(ret) == 37)
	system("cat valgrind.out");
    cr_assert_neq(WEXITSTATUS(ret), 37, "Valgrind reported errors");
    cr_assert_eq(WEXITSTATUS(ret), 0, "Server exit status was not 0");
}

Test(student_suite, 01_connect, .init = init, .fini = fini, .timeout = 5) {
    fprintf(stderr, "server_suite/01_connect\n");
    int ret = system("util/client -p 9999 </dev/null | grep 'Connected to server'");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}

// Test(student_suite, 02_connect, .init = init, .fini = fini, .timeout = 5) {
//     fprintf(stderr, "server_suite/01_connect\n");
//     int ret = system("util/client -p 9999 -q < tests/input02connect > tests/output02connect");
//     cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
// }

// Test(student_suite, 03_connect, .init = init, .fini = fini, .timeout = 5) {
//     fprintf(stderr, "server_suite/01_connect\n");
//     int ret = system("util/client -p 9999 -q < tests/input03connect > tests/output03connect");
//     cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
// }

Test(student_suite, 02_connect_multithreaded, .init = init, .fini = fini, .timeout = 5) {
    fprintf(stderr, "server_suite/02_connect\n");

    pthread_t register_threads[2];

    int randomData = 5;

    pthread_create(&register_threads[0], NULL, input_1, (void*)&randomData);
    pthread_create(&register_threads[1], NULL, input_2, (void*)&randomData);

    pthread_join(register_threads[0], NULL);
    pthread_join(register_threads[1], NULL);

    sleep(2);

    cr_assert_eq(ret1, 0, "expected %d, was %d\n", 0, ret1);
    cr_assert_eq(ret2, 0, "expected %d, was %d\n", 0, ret2);

}

Test(student_suite, 04_connect, .init = init, .fini = fini, .timeout = 10) {
    fprintf(stderr, "server_suite/04_connect\n");
    sleep(4);
    int ret = system("util/client -p 9999 -q < tests/input04connect > tests/output04connect");
    cr_assert_eq(ret, 0, "expected %d, was %d\n", 0, ret);
}