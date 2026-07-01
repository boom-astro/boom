# Stage 0, "build-stage", based on Node.js, to build and compile the frontend
FROM oven/bun AS build-stage

WORKDIR /app

COPY package.json /app/
COPY bun.lock /app/

RUN bun install

COPY ./ /app/

# Set API domain from build arg and expose it as VITE_ env var for build time
ARG BOOM_API__DOMAIN
ARG BOOM_SANDBOX_API__DOMAIN
ARG BOOM_KAFKA__DOMAIN
ARG VITE_PRERELEASE_MODE
ARG VITE_PUBLIC_POSTHOG_KEY
ARG VITE_PUBLIC_POSTHOG_HOST

ENV VITE_API_PROXY_TARGET=${BOOM_API__DOMAIN}
ENV VITE_SANDBOX_API_PROXY_TARGET=${BOOM_SANDBOX_API__DOMAIN}
ENV VITE_KAFKA_DOMAIN=${BOOM_KAFKA__DOMAIN}
ENV VITE_PRERELEASE_MODE=${VITE_PRERELEASE_MODE}
ENV VITE_PUBLIC_POSTHOG_KEY=${VITE_PUBLIC_POSTHOG_KEY}
ENV VITE_PUBLIC_POSTHOG_HOST=${VITE_PUBLIC_POSTHOG_HOST}

RUN bun run build


# Stage 1, based on Nginx, to have only the compiled app, ready for production with Nginx
FROM nginx:1

ARG BOOM_API__DOMAIN
ARG BOOM_SANDBOX_API__DOMAIN
ENV BOOM_API__DOMAIN=${BOOM_API__DOMAIN}
ENV BOOM_SANDBOX_API__DOMAIN=${BOOM_SANDBOX_API__DOMAIN}
ENV VITE_API_PROXY_TARGET=${BOOM_API__DOMAIN}
ENV VITE_SANDBOX_API_PROXY_TARGET=${BOOM_SANDBOX_API__DOMAIN}

COPY --from=build-stage /app/dist/ /usr/share/nginx/html

COPY ./config/nginx.conf /etc/nginx/conf.d/default.conf
COPY ./config/nginx-backend-not-found.conf /etc/nginx/extra-conf.d/backend-not-found.conf

# Set and log the API origins at container start (runtime env is honored)
RUN <<'SCRIPT' cat > /docker-entrypoint.d/00-set-api-origin.sh
#!/bin/sh
set -e
ORIGIN=https://${VITE_API_PROXY_TARGET}
SANDBOX_ORIGIN=https://${VITE_SANDBOX_API_PROXY_TARGET:-$VITE_API_PROXY_TARGET}
sed -i "s|__API_ORIGIN__|$ORIGIN|g" /etc/nginx/conf.d/default.conf
sed -i "s|__SANDBOX_API_ORIGIN__|$SANDBOX_ORIGIN|g" /etc/nginx/conf.d/default.conf
echo "[startup] API origin (proxy_pass): $ORIGIN"
echo "[startup] Sandbox API origin (proxy_pass): $SANDBOX_ORIGIN"
SCRIPT
RUN chmod +x /docker-entrypoint.d/00-set-api-origin.sh

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
