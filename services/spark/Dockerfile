FROM apache/spark:3.3.3-python3

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y wget curl tar gzip gettext-base netcat-openbsd sudo && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$JAVA_HOME/bin
ENV SPARK_HOME=/opt/spark
ENV RANGER_VERSION=2.1.0
ENV RANGER_HIVE_PLUGIN_HOME=/opt/ranger-hive-plugin

# Create spark_user with UID 1000 and add to sudo group
RUN useradd -u 1000 -ms /bin/bash spark_user && \
    usermod -aG sudo spark_user && \
    echo "spark_user ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    mkdir -p /opt/ranger-hive-plugin && \
    chown -R spark_user:spark_user /opt/spark && \
    chown -R spark_user:spark_user /opt/ranger-hive-plugin

# Copy and install Ranger Hive Plugin from local file
COPY ranger-${RANGER_VERSION}-hive-plugin.tar.gz /tmp/ranger-${RANGER_VERSION}-hive-plugin.tar.gz
RUN tar -xzf /tmp/ranger-${RANGER_VERSION}-hive-plugin.tar.gz -C /tmp && \
    cp -r /tmp/ranger-${RANGER_VERSION}-hive-plugin/* /opt/ranger-hive-plugin/ && \
    rm -rf /tmp/ranger-${RANGER_VERSION}-hive-plugin* && \
    rm -f /tmp/ranger-${RANGER_VERSION}-hive-plugin.tar.gz && \
    chown -R spark_user:spark_user /opt/ranger-hive-plugin

# Ensure Spark jars directory exists and copy Ranger plugin JARs
RUN mkdir -p $SPARK_HOME/jars && \
    cp /opt/ranger-hive-plugin/lib/*.jar $SPARK_HOME/jars/ && \
    chown spark_user:spark_user $SPARK_HOME/jars/*.jar

# Download additional required JARs
RUN curl -L -o $SPARK_HOME/jars/commons-lang-2.6.jar \
    https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar && \
    curl -L -o $SPARK_HOME/jars/log4j-api-2.13.3.jar \
    https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.13.3/log4j-api-2.13.3.jar && \
    curl -L -o $SPARK_HOME/jars/jackson-jaxrs-json-provider-2.13.3.jar \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-json-provider/2.13.3/jackson-jaxrs-json-provider-2.13.3.jar && \
    curl -L -o $SPARK_HOME/jars/jackson-jaxrs-base-2.13.3.jar \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-base/2.13.3/jackson-jaxrs-base-2.13.3.jar && \
    chown spark_user:spark_user $SPARK_HOME/jars/commons-lang-2.6.jar && \
    chown spark_user:spark_user $SPARK_HOME/jars/log4j-api-2.13.3.jar && \
    chown spark_user:spark_user $SPARK_HOME/jars/jackson-jaxrs-json-provider-2.13.3.jar && \
    chown spark_user:spark_user $SPARK_HOME/jars/jackson-jaxrs-base-2.13.3.jar

# Create Ranger configuration directories
RUN mkdir -p /opt/spark/conf/ranger && \
    chown -R spark_user:spark_user /opt/spark/conf/ranger

# Add run.sh script
ADD run.sh /run.sh
RUN chmod a+x /run.sh

# Set the default user
USER spark_user

# Expose ports
EXPOSE 10000 4040

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:4040 || exit 1