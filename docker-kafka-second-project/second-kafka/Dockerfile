# Use Node.js base image
FROM node:18

# Set working directory
WORKDIR /app

# Copy only package.json files first (for caching npm install)
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the project
COPY . .

# Expose if your app runs on a port (not needed for Kafka producer/consumer only)
EXPOSE 8080

# Example: run producer.js (you can change this to consumer.js, admin.js, etc.)
CMD ["node", "producer.js"]
