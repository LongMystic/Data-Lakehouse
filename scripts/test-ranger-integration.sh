#!/bin/bash

echo "🧪 Testing Ranger Integration..."

# Check if containers are running
echo "1. Checking if containers are running..."
if docker ps | grep -q "spark-thrift-server"; then
    echo "✅ Spark Thrift Server is running"
else
    echo "❌ Spark Thrift Server is not running"
    exit 1
fi

if docker ps | grep -q "ranger-admin"; then
    echo "✅ Ranger Admin is running"
else
    echo "❌ Ranger Admin is not running"
    exit 1
fi

if docker ps | grep -q "hive-metastore"; then
    echo "✅ Hive Metastore is running"
else
    echo "❌ Hive Metastore is not running"
    exit 1
fi

# Test Ranger Admin accessibility
echo "2. Testing Ranger Admin accessibility..."
if curl -s http://localhost:6080 > /dev/null; then
    echo "✅ Ranger Admin is accessible at http://localhost:6080"
else
    echo "❌ Ranger Admin is not accessible"
fi

# Test Spark Thrift Server
echo "3. Testing Spark Thrift Server..."
if nc -z localhost 10000; then
    echo "✅ Spark Thrift Server is listening on port 10000"
else
    echo "❌ Spark Thrift Server is not listening on port 10000"
fi

# Check Ranger plugin in Spark container
echo "4. Checking Ranger plugin in Spark container..."
if docker exec spark-thrift-server ls -la /opt/ranger-hive-plugin/enable-hive-plugin.sh > /dev/null 2>&1; then
    echo "✅ Ranger Hive Plugin is installed in Spark container"
else
    echo "❌ Ranger Hive Plugin is not found in Spark container"
fi

# Check Ranger JARs in Spark
echo "5. Checking Ranger JARs in Spark..."
if docker exec spark-thrift-server ls -la /opt/spark/jars/ | grep -q "ranger"; then
    echo "✅ Ranger JARs are present in Spark jars directory"
else
    echo "❌ Ranger JARs are not found in Spark jars directory"
fi

echo ""
echo "🎉 Ranger Integration Test Complete!"
echo ""
echo "Next steps:"
echo "1. Access Ranger Admin: http://localhost:6080"
echo "2. Create Hive service in Ranger Admin"
echo "3. Configure policies for databases, tables, and columns"
echo "4. Test authorization with: beeline -u jdbc:hive2://localhost:10000" 
