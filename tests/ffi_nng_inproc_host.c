#define _POSIX_C_SOURCE 200809L

#include <nng/nng.h>
#include <nng/protocol/pubsub0/pub.h>
#include <nng/protocol/pubsub0/sub.h>

#include "veloflux_ffi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static void sleep_ms(long milliseconds) {
    struct timespec duration = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000L,
    };
    nanosleep(&duration, NULL);
}

static int fail_nng(const char *operation, int error) {
    fprintf(stderr, "%s failed: %s\n", operation, nng_strerror(error));
    return 1;
}

static int fail_ffi(const char *operation, int code) {
    fprintf(stderr, "%s failed with FFI status %d\n", operation, code);
    return 1;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s <config-path> <input-url> <output-url>\n", argv[0]);
        return 2;
    }

    const char *config_path = argv[1];
    const char *input_url = argv[2];
    const char *output_url = argv[3];
    const char *topic = "topic/can";
    const char *input_message = "topic/can:{\"v\":42}";
    const char *expected_output = "topic/can:[{\"v\":42}]";
    nng_socket publisher = NNG_SOCKET_INITIALIZER;
    nng_socket subscriber = NNG_SOCKET_INITIALIZER;
    veloflux_handle_t *handle = NULL;
    int error;
    int exit_code = 1;

    if ((error = nng_pub0_open(&publisher)) != 0) {
        return fail_nng("nng_pub0_open", error);
    }
    if ((error = nng_sub0_open(&subscriber)) != 0) {
        fail_nng("nng_sub0_open", error);
        nng_close(publisher);
        return 1;
    }

    if ((error = nng_socket_set(subscriber, NNG_OPT_SUB_SUBSCRIBE, topic, strlen(topic))) != 0) {
        fail_nng("nng_setopt subscribe", error);
        goto cleanup;
    }
    if ((error = nng_socket_set_ms(subscriber, NNG_OPT_RECVTIMEO, 100)) != 0) {
        fail_nng("nng_setopt recv timeout", error);
        goto cleanup;
    }
    if ((error = nng_socket_set_ms(publisher, NNG_OPT_SENDTIMEO, 100)) != 0) {
        fail_nng("nng_setopt send timeout", error);
        goto cleanup;
    }
    if ((error = nng_listen(publisher, input_url, NULL, 0)) != 0) {
        fail_nng("nng_listen input", error);
        goto cleanup;
    }
    if ((error = nng_listen(subscriber, output_url, NULL, 0)) != 0) {
        fail_nng("nng_listen output", error);
        goto cleanup;
    }

    error = veloflux_start(config_path, &handle);
    if (error != VELOFLUX_FFI_OK) {
        fail_ffi("veloflux_start", error);
        goto cleanup;
    }

    /* The workflow configures the manager while this process retries the message. */
    for (int attempt = 0; attempt < 100; ++attempt) {
        error = nng_send(publisher, (void *)input_message, strlen(input_message), 0);
        if (error != 0 && error != NNG_ETIMEDOUT) {
            fail_nng("nng_send", error);
            goto cleanup;
        }

        void *received = NULL;
        size_t received_size = 0;
        error = nng_recv(subscriber, &received, &received_size, 0);
        if (error == 0) {
            int matches = received_size == strlen(expected_output) &&
                          memcmp(received, expected_output, received_size) == 0;
            nng_free(received, received_size);
            if (!matches) {
                fprintf(stderr, "unexpected inproc output\n");
                goto cleanup;
            }
            exit_code = 0;
            break;
        }
        if (error != NNG_ETIMEDOUT) {
            fail_nng("nng_recv", error);
            goto cleanup;
        }
        sleep_ms(100);
    }

    if (exit_code != 0) {
        fprintf(stderr, "timed out waiting for the nng inproc output\n");
    }

cleanup:
    if (handle != NULL) {
        int stop_status = veloflux_stop(&handle);
        if (stop_status != VELOFLUX_FFI_OK) {
            fail_ffi("veloflux_stop", stop_status);
            exit_code = 1;
        }
    }
    nng_close(subscriber);
    nng_close(publisher);
    return exit_code;
}
