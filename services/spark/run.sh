#!/bin/bash

# Note: We skip the enable-hive-plugin.sh script because:
# 1. It's designed for Hive installation, not Spark
# 2. We're manually configuring Ranger integration in our setup
# 3. The JARs are already copied and configured in the Dockerfile

echo "Ranger Hive Plugin configuration is handled manually in our setup."
echo "JARs are already copied to /opt/spark/jars/"
echo "Configuration files are mounted via docker-compose.yml"
echo "Ranger Hive Plugin is ready for use!" 