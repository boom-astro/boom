# Stage 0, "deps", install dependencies once and share across build/dev stages
FROM oven/bun AS deps

WORKDIR /app

# NOTE: build context is the repo root (see docker-compose*.yaml), so frontend
# paths are prefixed with frontend/ and docs/ is reachable for bundling.
COPY frontend/package.json /app/
COPY frontend/bun.lock /app/

RUN bun install


# Stage 1, "build-stage", based on Node.js, to build and compile the frontend
FROM deps AS build-stage

COPY frontend/ /app/
# Bundle the canonical doc so the Kafka docs page renders it from the build
# instead of fetching it from GitHub at runtime (imported via ?raw).
COPY docs/babamul.md /app/docs/babamul.md

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
FROM nginx:1 AS prod

ARG BOOM_API__DOMAIN
ARG BOOM_SANDBOX_API__DOMAIN
ENV BOOM_API__DOMAIN=${BOOM_API__DOMAIN}
ENV BOOM_SANDBOX_API__DOMAIN=${BOOM_SANDBOX_API__DOMAIN}
ENV VITE_API_PROXY_TARGET=${BOOM_API__DOMAIN}
ENV VITE_SANDBOX_API_PROXY_TARGET=${BOOM_SANDBOX_API__DOMAIN}

COPY --from=build-stage /app/dist/ /usr/share/nginx/html

COPY frontend/config/nginx.conf /etc/nginx/conf.d/default.conf
COPY frontend/config/nginx-backend-not-found.conf /etc/nginx/extra-conf.d/backend-not-found.conf

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
# Set and log the API origin at container start (runtime env is honored)
RUN printf '#!/bin/sh\nset -e\n# Only prepend a scheme when the target does not already carry one, so a\n# fully-qualified URL (https://api.example.com) is not turned into https://https://...\ncase "$VITE_API_PROXY_TARGET" in\n  http://*|https://*) ORIGIN="$VITE_API_PROXY_TARGET" ;;\n  *) ORIGIN="https://$VITE_API_PROXY_TARGET" ;;\nesac\n# Replace placeholder if still present\nsed -i "s|__API_ORIGIN__|$ORIGIN|g" /etc/nginx/conf.d/default.conf\nAPI_ORIGIN=$(awk '\''$1=="proxy_pass"{gsub(";","",$2);print $2}'\'' /etc/nginx/conf.d/default.conf)\necho "[startup] VITE_API_PROXY_TARGET=${VITE_API_PROXY_TARGET:-<unset>}"\necho "[startup] API origin (proxy_pass): $API_ORIGIN"\n' > /docker-entrypoint.d/00-set-api-origin.sh \
	&& chmod +x /docker-entrypoint.d/00-set-api-origin.sh

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]


# Dev stage: run the Vite dev server with hot module reload. Reuses the
# installed deps (no production build); source is bind-mounted at runtime (see
# docker-compose.override.yaml) so edits trigger live rebuilds in the container.
FROM deps AS dev

COPY frontend/ /app/
# Baked in for builds without the bind mount; docker-compose.override.yaml also
# mounts the live doc over this path for dev.
COPY docs/babamul.md /app/docs/babamul.md

EXPOSE 5173

CMD ["bun", "run", "dev", "--host", "0.0.0.0", "--port", "5173"]
