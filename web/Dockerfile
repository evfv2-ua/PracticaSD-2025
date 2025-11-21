# Stage 1: build frontend
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

# Stage 2: serve static assets
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
# default nginx listens on 80
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
