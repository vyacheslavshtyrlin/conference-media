#!/bin/sh
# Ensures a placeholder self-signed cert exists BEFORE nginx starts, so the
# :443 server block can boot even when Let's Encrypt has not issued the real
# cert yet. Real cert replaces the placeholder once certbot succeeds.
# Idempotent: skips when certbot already manages the domain.
set -e

DOMAIN="${MEDIA_DOMAIN:?MEDIA_DOMAIN is required}"
LIVE_DIR="/etc/letsencrypt/live/${DOMAIN}"
RENEWAL_CONF="/etc/letsencrypt/renewal/${DOMAIN}.conf"

if [ -f "$RENEWAL_CONF" ]; then
    echo "[init-cert] real cert present for ${DOMAIN}; nothing to do"
    exit 0
fi

echo "[init-cert] no real cert yet; generating placeholder for ${DOMAIN}"
apk add --no-cache openssl >/dev/null
mkdir -p "$LIVE_DIR"
openssl req -x509 -nodes -newkey rsa:2048 \
    -keyout "${LIVE_DIR}/privkey.pem" \
    -out "${LIVE_DIR}/fullchain.pem" \
    -subj "/CN=${DOMAIN}" -days 1 >/dev/null 2>&1
echo "[init-cert] placeholder ready at ${LIVE_DIR}"
