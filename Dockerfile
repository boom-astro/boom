# Stage 0, "build-stage", based on Node.js, to build and compile the frontend
FROM oven/bun AS build-stage

WORKDIR /app

COPY package.json /app/
COPY bun.lock /app/

RUN bun install

COPY ./ /app/

# Set API domain from build arg and expose it as VITE_ env var for build time
ARG BOOM_API__DOMAIN=${BOOM_API__DOMAIN}
ARG PRERELEASE_MODE=${PRERELEASE_MODE:-false}
ENV VITE_API_PROXY_TARGET=${BOOM_API__DOMAIN}
ENV VITE_PRERELEASE_MODE=${PRERELEASE_MODE}

RUN bun run build


# Stage 1, based on Nginx, to have only the compiled app, ready for production with Nginx
FROM nginx:1

COPY --from=build-stage /app/dist/ /usr/share/nginx/html

COPY ./config/nginx.conf /etc/nginx/conf.d/default.conf
COPY ./config/nginx-backend-not-found.conf /etc/nginx/extra-conf.d/backend-not-found.conf
