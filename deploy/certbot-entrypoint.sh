#!/bin/sh
# Issues the real Let's Encrypt cert (once) then renews in a loop.
set -e

DOMAIN="${MEDIA_DOMAIN:?MEDIA_DOMAIN is required}"
EMAIL="${CERTBOT_EMAIL:?CERTBOT_EMAIL is required}"
WEBROOT="/var/www/certbot"
RENEWAL_CONF="/etc/letsencrypt/renewal/${DOMAIN}.conf"

# ждём, пока gateway не поднимет :80 и не начнёт отдавать webroot для ACME.
echo "[certbot] waiting for gateway :80 ..."
sleep 20

if [ ! -f "$RENEWAL_CONF" ]; then
    echo "[certbot] issuing certificate for ${DOMAIN}"
    # убираем плейсхолдер, чтобы certbot создал чистый lineage.
    rm -f "/etc/letsencrypt/live/${DOMAIN}/fullchain.pem" \
          "/etc/letsencrypt/live/${DOMAIN}/privkey.pem"
    rmdir "/etc/letsencrypt/live/${DOMAIN}" 2>/dev/null || true

    if certbot certonly --webroot -w "$WEBROOT" \
        -d "$DOMAIN" --email "$EMAIL" \
        --agree-tos --no-eff-email --non-interactive; then
        echo "[certbot] certificate issued for ${DOMAIN}"
    else
        echo "[certbot] issuance failed; nginx keeps serving placeholder"
        exit 0
    fi
else
    echo "[certbot] lineage already exists for ${DOMAIN}"
fi

echo "[certbot] starting renewal loop"
trap exit TERM
while :; do
    certbot renew --webroot -w "$WEBROOT" --quiet || true
    sleep 12h
done
