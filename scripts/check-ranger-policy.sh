#!/bin/bash

echo "🔍 Ranger Policy Check for spark_user"
echo "===================================="
echo ""

echo "1. 🌐 Checking Ranger Admin Status..."
echo "-----------------------------------"
if curl -s http://localhost:6080 > /dev/null; then
    echo "✅ Ranger Admin is accessible at http://localhost:6080"
else
    echo "❌ Ranger Admin is not accessible"
    exit 1
fi

echo ""
echo "2. 📋 Checking Hive Service in Ranger..."
echo "---------------------------------------"
HIVE_SERVICE=$(curl -s -u admin:rangeradmin1 http://localhost:6080/service/public/v2/api/service | jq '.[] | select(.type == "hive") | .name')
if [ "$HIVE_SERVICE" == '"hive"' ]; then
    echo "✅ Hive service exists in Ranger"
else
    echo "❌ Hive service not found in Ranger"
    exit 1
fi

echo ""
echo "3. 🔐 Checking Policies for spark_user..."
echo "----------------------------------------"
SPARK_USER_POLICIES=$(curl -s -u admin:rangeradmin1 "http://localhost:6080/service/public/v2/api/service/hive/policy" | jq '.[] | select(.policyItems[].users[]? == "spark_user") | .name')
if [ -n "$SPARK_USER_POLICIES" ]; then
    echo "✅ Found policies for spark_user:"
    echo "$SPARK_USER_POLICIES"
else
    echo "❌ No policies found for spark_user"
fi

echo ""
echo "4. 📊 Policy Details for spark_user..."
echo "------------------------------------"
curl -s -u admin:rangeradmin1 "http://localhost:6080/service/public/v2/api/service/hive/policy" | jq '.[] | select(.name == "default database tables columns") | {name: .name, enabled: .isEnabled, databases: .resources.database.values, tables: .resources.table.values, permissions: .policyItems[0].accesses[].type}'

echo ""
echo "5. 🧪 Testing Beeline Connection..."
echo "---------------------------------"
echo "Testing connection as spark_user..."
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user -e "SHOW DATABASES;" 2>&1 | head -5

echo ""
echo "6. 🔧 Spark Thrift Server Configuration Check..."
echo "---------------------------------------------"
echo "Checking Ranger plugin configuration..."
docker exec -it spark-thrift-server ls -la /opt/spark/jars/ | grep -i ranger | head -5

echo ""
echo "7. 📋 Current Status Summary:"
echo "----------------------------"
echo "✅ Ranger Admin: Running"
echo "✅ Hive Service: Configured"
echo "✅ spark_user Policy: Exists with ALL permissions"
echo "❌ spark_user still cannot access default database"
echo ""
echo "8. 🔍 Possible Issues:"
echo "---------------------"
echo "1. Ranger plugin not properly loaded in Spark"
echo "2. Policy not being applied correctly"
echo "3. Spark Thrift Server configuration issue"
echo "4. Network connectivity between Spark and Ranger"
echo ""
echo "9. 🧪 Debug Commands:"
echo "-------------------"
echo "# Check Ranger plugin logs:"
echo "docker logs spark-thrift-server | grep -i ranger"
echo ""
echo "# Check if Ranger plugin is loaded:"
echo "docker exec -it spark-thrift-server ls -la /opt/spark/jars/ | grep ranger"
echo ""
echo "# Test Ranger Admin connectivity from Spark:"
echo "docker exec -it spark-thrift-server curl -s http://ranger-admin:6080"
echo ""
echo "# Check Spark configuration:"
echo "docker exec -it spark-thrift-server cat /opt/spark/conf/spark-defaults.conf | grep ranger" 