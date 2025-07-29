# 🔐 True Accounts Guide

## 📋 **Available System Accounts**

Based on your system configuration, here are the **true accounts** available:

### **🔧 Spark Container Accounts:**
- **`spark_user`** (UID: 1000) - **Main Spark user**
- **`spark`** (UID: 185) - Alternative Spark user

### **🔧 Hive Container Accounts:**
- **`hive`** (UID: 1000) - **Main Hive user**

### **🔧 System Accounts:**
- **`admin`** - System admin (but not for database access)

## 🎯 **Important: Spark Thrift Server Configuration**

Your Spark Thrift Server is configured with:
```bash
--proxy-user spark_user
```

This means:
- ✅ **Spark Thrift Server runs as `spark_user`**
- ✅ **All connections are proxied through `spark_user`**
- ✅ **Ranger sees all connections as `spark_user`**

## 🧪 **Testing True Accounts**

### **1. Test with spark_user (Recommended):**
```bash
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user
```

### **2. Test with spark:**
```bash
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark
```

### **3. Test with hive:**
```bash
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n hive
```

## ⚠️ **Expected Behavior**

**All users will show as `spark_user` in Ranger** because of the `--proxy-user spark_user` configuration:

```
Error: Could not open client transport with JDBC Uri: jdbc:hive2://spark-thrift-server:10000: 
Failed to open new session: org.apache.kyuubi.plugin.spark.authz.AccessControlException: 
Permission denied: user [spark_user] does not have [_any] privilege on [default]
```

This is **normal and expected** - Ranger sees all connections as `spark_user`.

## 🔧 **Ranger Policy Configuration**

Since all connections are proxied through `spark_user`, you only need **one policy**:

### **Create Policy for spark_user:**
- **Policy Name**: `spark_user_access`
- **Database**: `default`
- **Table**: `*`
- **Column**: `*`
- **User**: `spark_user`
- **Permissions**: `SELECT, CREATE, INSERT, UPDATE, DELETE, ALL`

## 🧪 **Quick Test Commands**

### **Test All Available Users:**
```bash
# Test spark_user (main user)
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user

# Test spark (alternative)
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark

# Test hive
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n hive
```

### **Test from Spark Container:**
```bash
# Test from Spark container
docker exec -it spark-thrift-server /opt/spark/bin/beeline -u "jdbc:hive2://localhost:10000" -n spark_user
```

## 📊 **User Verification Commands**

### **Check Available Users:**
```bash
# Check Spark container users
docker exec -it spark-thrift-server cat /etc/passwd | grep -E "(spark|hive|hadoop|admin)"

# Check Hive container users
docker exec -it hive-metastore cat /etc/passwd | grep -E "(hive|hadoop|admin|spark)"

# Check current user in Spark container
docker exec -it spark-thrift-server whoami
```

### **Check Spark Thrift Server Configuration:**
```bash
# Check proxy user configuration
docker logs spark-thrift-server | grep -i "proxy\|user" | head -5
```

## 🎯 **Summary**

### **✅ True Accounts:**
1. **`spark_user`** - Main Spark user (UID: 1000)
2. **`spark`** - Alternative Spark user (UID: 185)
3. **`hive`** - Hive user (UID: 1000 in Hive container)

### **✅ Ranger Configuration:**
- **All connections are proxied through `spark_user`**
- **Only need one policy for `spark_user`**
- **All users will show as `spark_user` in audit logs**

### **✅ Recommended Usage:**
```bash
# Use spark_user for all connections
docker exec -it hive-metastore /opt/hive/bin/beeline -u "jdbc:hive2://spark-thrift-server:10000" -n spark_user
```

## 🚀 **Next Steps**

1. **Configure Ranger policy for `spark_user`** in Admin UI (http://localhost:6080)
2. **Test connection** with `spark_user`
3. **Monitor audit logs** - all will show as `spark_user`

**The `spark_user` account is your main account for accessing Beeline!** 🎉 