# FROM alpine:latest
FROM ubuntu:latest

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y upgrade && apt-get install -y wget git build-essential

WORKDIR /home

# hadoop - only needed for the .jar and .h files
RUN \
    wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz -O hadoop.tar.gz && \
    mkdir hadoop && \
    tar -xvzf hadoop.tar.gz -C hadoop && \
    mv ./hadoop/hadoop-3.3.1/* ./hadoop/ && \
    rm hadoop.tar.gz

# rocksdb
RUN \
    apt-get update && \
    apt-get install -y openjdk-8-jdk \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev && \
    wget https://github.com/facebook/rocksdb/archive/refs/tags/v7.2.2.tar.gz -O rocksdb.tar.gz && \
    mkdir rocksdb && \
    tar -xvzf rocksdb.tar.gz -C ./rocksdb && \
    mv ./rocksdb/rocksdb-7.2.2/* ./rocksdb/ && \
    rm rocksdb.tar.gz && \
    echo "source /home/.rockshdfs_commons" >> ~/.bashrc && \
    rm -rf /var/lib/apt/lists/*

RUN ["/bin/bash", "-c", "source ~/.bashrc"]

WORKDIR /home/rocksdb

RUN \
    export HADOOP_HOME=/home/hadoop && \
    export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64 && \
    export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native && \
    export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob` && \
    for f in `find $HADOOP_HOME/share/hadoop/hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
    for f in `find $HADOOP_HOME/share/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
    for f in `find $HADOOP_HOME/share/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done && \
    cd ./plugin/ && \
    git clone https://github.com/asu-idi/rocksdb-hdfs hdfs && \
    cd .. && \
    make clean && DEBUG_LEVEL=0 ROCKSDB_PLUGINS="hdfs" make -j$(nproc) install && \
    cp make_config.mk .. && \
    rm -r /home/rocksdb/

EXPOSE 9090
EXPOSE 34728

WORKDIR /home

COPY src/startup.sh /home/startup.sh
COPY src/Makefile /home/Makefile
COPY src/server.cc /home/server.cc

RUN \
    chmod +x /home/startup.sh

CMD /bin/bash -c '/home/startup.sh'
