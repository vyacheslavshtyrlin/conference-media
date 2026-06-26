#!/bin/sh
# Periodic nginx reload so renewed Let's Encrypt certs are picked up.
# Mounted into /docker-entrypoint.d/ of the official nginx image (sourced on startup).
set -e

(
    sleep 60
    while :; do
        nginx -s reload 2>/dev/null || true
        sleep 12h
    done
) &
