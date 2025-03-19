#include "validate_args.h"

#include <stdlib.h>
#include <string.h>


int validate_args(int argc, char** argv) {
    if (argc < 3) {
        return -1;
    }

    if (strncmp(argv[1], "-p", strlen("-p")) != 0) {
        return -1;
    }

    int port = atoi(argv[2]);

    if (port <= 0) {
        return -1;
    }

    return port;

}