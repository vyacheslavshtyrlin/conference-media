#!/bin/sh
# Issues the real Let's Encrypt cert (until success) then renews periodically.
# Runs in its own container; nginx picks up changes via the reload loop.
#
# IMPORTANT: on failure we do NOT exit. With `restart: unless-stopped` an exit
# would cause Docker to restart the container instantly and re-run issuance,
# hammering Let's Encrypt and burning its "5 failed validations / hostname /
# hour" rate limit. Instead the container stays up and retries on a schedule.
set -e

DOMAIN="${MEDIA_DOMAIN:?MEDIA_DOMAIN is required}"
EMAIL="${CERTBOT_EMAIL:?CERTBOT_EMAIL is required}"
WEBROOT="/var/www/certbot"
RENEWAL_CONF="/etc/letsencrypt/renewal/${DOMAIN}.conf"
RETRY_INTERVAL="${CERTBOT_RETRY_INTERVAL:-1800}"   # 30 min by default

# Выпуск (первый раз) или продление. Возвращает ненулевой код при неудаче,
# но вызывающий через `||` гасит ошибку, чтобы контейнер не упал.
issue_or_renew() {
    if [ -f "$RENEWAL_CONF" ]; then
        certbot renew --webroot -w "$WEBROOT" --quiet
        return $?
    fi
    # Первый выпуск: убираем плейсхолдер, чтобы certbot создал чистый lineage.
    rm -f "/etc/letsencrypt/live/${DOMAIN}/fullchain.pem" \
          "/etc/letsencrypt/live/${DOMAIN}/privkey.pem"
    rmdir "/etc/letsencrypt/live/${DOMAIN}" 2>/dev/null || true
    echo "[certbot] issuing certificate for ${DOMAIN}"
    certbot certonly --webroot -w "$WEBROOT" \
        -d "$DOMAIN" --email "$EMAIL" \
        --agree-tos --no-eff-email --non-interactive
}

# Ждём, пока gateway не поднимёт :80 и не начнёт отдавать webroot для ACME.
echo "[certbot] waiting for gateway :80 ..."
sleep 20

# Первая попытка. При неудаче НЕ выходим — уходим в ретраи по расписанию.
if issue_or_renew; then
    echo "[certbot] ok"
else
    echo "[certbot] attempt failed; will retry in ${RETRY_INTERVAL}s"
fi

trap exit TERM
while :; do
    sleep "$RETRY_INTERVAL"
    if issue_or_renew; then
        echo "[certbot] ok"
    else
        echo "[certbot] attempt failed; will retry in ${RETRY_INTERVAL}s"
    fi
done
