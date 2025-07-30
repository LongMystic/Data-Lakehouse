#!/bin/bash

# Build Spark Custom Image with Ranger Integration
echo "Building Spark Custom Image with Ranger Integration..."

# Navigate to the spark service directory
cd services/spark

# Check if the Ranger plugin file exists
if [ ! -f "ranger-2.1.0-hive-plugin.tar.gz" ]; then
    echo "❌ Error: ranger-2.1.0-hive-plugin.tar.gz not found in services/spark/"
    echo "Please ensure the file is present before building the image."
    exit 1
fi

echo "✅ Found ranger-2.1.0-hive-plugin.tar.gz - using local file"

# Build the Docker image
echo "Building Docker image..."
docker build -t spark-custom:v2.0 .

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "✅ Spark Custom Image built successfully!"
    echo "Image: spark-custom:v2.0"
    echo ""
    echo "To start your lakehouse with Ranger integration:"
    echo "cd .. && docker-compose up -d"
else
    echo "❌ Failed to build Spark Custom Image"
    exit 1
fi 
