FROM node:22-alpine AS base
WORKDIR /app

FROM base AS deps
RUN apk add --no-cache python3 py3-pip make g++ linux-headers
COPY package*.json ./
RUN npm ci

FROM deps AS dev
COPY . .
EXPOSE 4000
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD wget -q --spider http://127.0.0.1:4000/health || exit 1
CMD ["npm", "run", "dev"]

FROM deps AS build
COPY . .
RUN npm run build
RUN npm prune --omit=dev

FROM node:22-alpine AS production
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/package*.json ./
COPY --from=build /app/dist ./dist
EXPOSE 4000
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD wget -q --spider http://127.0.0.1:4000/health || exit 1
CMD ["npm", "run", "start"]
