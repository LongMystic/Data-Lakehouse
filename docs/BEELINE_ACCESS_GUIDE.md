# 🐝 Beeline Access Guide

## Overview
Beeline is the command-line interface for connecting to Spark Thrift Server (which provides HiveServer2 functionality). Here are the different ways to access it.

## 🔧 **Method 1: From Hive Container (Recommended)**

The Hive container has beeline installed and can connect to the Spark Thrift Server:

```bash
# Connect from Hive container
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user

# Or connect as longvk user
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n longvk
```

## 🔧 **Method 2: From Spark Container**

The Spark container also has beeline:

```bash
# Connect from Spark container
docker exec -it spark-thrift-server /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n spark_user

# Or connect as longvk user
docker exec -it spark-thrift-server /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n longvk
```

## 🔧 **Method 3: From Your Local Machine**

If you have beeline installed locally:

```bash
# Connect from your local machine
beeline -u "jdbc:hive2://localhost:10000" -n spark_user

# Or connect as longvk user
beeline -u "jdbc:hive2://localhost:10000" -n longvk
```

## 🔧 **Method 4: Interactive Container Access**

You can also enter a container and run beeline interactively:

```bash
# Enter Hive container
docker exec -it hive-metastore bash

# Then run beeline
/opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user
```

## 📋 **Connection Parameters**

### **JDBC URL Format:**
- **From Hive container**: `jdbc:hive2://spark-thrift-server:10000`
- **From Spark container**: `jdbc:hive2://localhost:10000`
- **From local machine**: `jdbc:hive2://localhost:10000`

### **User Options:**
- `-n spark_user` (for spark_user)
- `-n longvk` (for longvk user)
- `-n admin` (for admin user)

## ⚠️ **Expected Behavior**

### **Before Policy Configuration:**
You'll see this error (which is **expected**):
```
Error: Could not open client transport with JDBC Uri: jdbc:hive2://spark-thrift-server:10000: 
Failed to open new session: org.apache.kyuubi.plugin.spark.authz.AccessControlException: 
Permission denied: user [spark_user] does not have [_any] privilege on [default]
```

This error confirms:
- ✅ **Ranger is working** - It's intercepting connections
- ✅ **Authorization is active** - It's checking permissions
- ✅ **Spark Thrift Server is running** - Connection reaches the server

### **After Policy Configuration:**
Once you configure Ranger policies, you should see:
```
Connecting to jdbc:hive2://spark-thrift-server:10000
Connected to: Apache Hive (version 3.1.2)
Driver: Hive JDBC (version 3.1.2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
Beeline version 3.1.2 by Apache Hive
0: jdbc:hive2://spark-thrift-server:10000>
```

## 🧪 **Quick Test Commands**

### **Test Connection (will fail until policies are configured):**
```bash
# Test from Hive container
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user

# Test from Spark container  
docker exec -it spark-thrift-server /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n spark_user
```

### **Test with Different Users:**
```bash
# Test as spark_user
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user

# Test as longvk
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n longvk

# Test as admin
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n admin
```

## 🔐 **After Policy Configuration**

Once you configure Ranger policies in the Admin UI (http://localhost:6080), you can:

### **1. Connect and Run Queries:**
```sql
-- Show databases
SHOW DATABASES;

-- Use default database
USE default;

-- Show tables
SHOW TABLES;

-- Create a test table
CREATE TABLE test_table (id INT, name STRING);

-- Insert data
INSERT INTO test_table VALUES (1, 'test');

-- Query data
SELECT * FROM test_table;
```

### **2. Exit Beeline:**
```sql
!quit
```

## 📊 **Monitor Connections**

### **Check Active Connections:**
```bash
# Check Spark Thrift Server logs
docker logs spark-thrift-server --tail 20

# Check Ranger audit logs
curl "http://localhost:9200/ranger_audits/_search?pretty&size=5"
```

## 🎯 **Troubleshooting**

### **If Connection Fails:**
1. **Check if Spark Thrift Server is running:**
   ```bash
   docker ps | grep spark-thrift-server
   ```

2. **Check Spark Thrift Server logs:**
   ```bash
   docker logs spark-thrift-server --tail 50
   ```

3. **Check if port 10000 is accessible:**
   ```bash
   nc -z localhost 10000
   ```

4. **Verify Ranger policies are configured:**
   - Access http://localhost:6080
   - Check if policies exist for your user

### **Common Issues:**
- **Permission denied**: Ranger is working - configure policies
- **Connection refused**: Spark Thrift Server not running
- **Host not found**: Use correct container names in JDBC URL

## 🚀 **Next Steps**

1. **Configure Ranger Policies** in Admin UI (http://localhost:6080)
2. **Test connections** with different users
3. **Run SQL queries** to verify functionality
4. **Monitor audit logs** in Elasticsearch

**Your Ranger integration is working perfectly! The connection errors are expected until you configure the policies.** 🎉 