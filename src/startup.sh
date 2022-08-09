export HADOOP_HOME=/home/hadoop
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`

for f in `find $HADOOP_HOME/share/hadoop/hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done

cd /home
make server
./server 
