# Keystone frontend image: build the Vite bundle, serve it with unprivileged nginx.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY . .
# Vite inlines these at build time. The API URL is as seen from the browser;
# the other two are optional (see .env.example).
ARG VITE_API_BASE_URL=http://localhost:8000
ARG VITE_REPO_URL=
ARG VITE_PUBLIC_URL=
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL} \
    VITE_REPO_URL=${VITE_REPO_URL} \
    VITE_PUBLIC_URL=${VITE_PUBLIC_URL}
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
