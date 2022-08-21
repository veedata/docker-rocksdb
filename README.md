# Rocksdb (HDFS plugin) on docker
This repository is being explicity developed for a project and will not be appropriate for all users. The current actively developed branches are [Primary DB](https://github.com/veedata/docker-rocksdb/tree/primary-db) and [Secondary DB](https://github.com/veedata/docker-rocksdb/tree/secondary-db)

|PrimaryDB Docker Pulls   |Secondary Docker Pulls   |
| ---                     |---                      |
|[![PrimaryDB Docker Pulls](https://img.shields.io/docker/pulls/veedata/rocksdb-hdfs-primarydb.svg)]() | [![SecondaryDB Docker Pulls](https://img.shields.io/docker/pulls/veedata/rocksdb-hdfs-secondarydb.svg)]() |

<hr>

## Primary DB
This will open a primary instance of RocksDB on a pre-programmed path. The port exposed for connections is: `36728`

## Secondary DB
This will open a secondary instance of RocksDB on a pre-programmed path. The port exposed for connections is: `34728`

## Specifications
- Base image: Ubuntu 20.04 64 bit
- Java: openjdk-8-jdk
- Hadoop - v3.3.1
- RocksDB - v7.4.5

## To-Do
- Merge primary and secondary db to run from the same docker image instead of 2 different ones
- Connect with ldb tool for maximum flexibility
- Reduce image size
- Make it modular, i.e. in case some pluging isnt required, image can be smaller in size and/or it can be excluded while docker run.

## Warnings
Do wait for the next couple of updates. In case someone needs this instantly, I would recommend a fork and developing it for your own!
