[![Build Status](https://travis-ci.org/salesforce/mirus.svg?branch=master)](https://travis-ci.org/salesforce/mirus)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

# Mirus

A tool for distributed, high-volume replication between [Apache Kafka](https://kafka.apache.org/documentation) clusters based
on [Kafka Connect](https://kafka.apache.org/documentation/#connect). Designed for easy
operation in a high-throughput, multi-cluster environment.

## Features

- **Dynamic Configuration:** Uses the Kafka Connect REST API for dynamic API-driven configuration
- **Precise Replication:** Supports a regex whitelist and an explicit topic whitelist
- **Simple Management for Multiple Kafka Clusters:** Supports multiple source clusters with one Worker process
- **Continuous Ingestion:** Continues consuming from the source cluster while flushing and committing offsets
- **Built for Dynamic Kafka Clusters:** Able to handle topics and partitions being created and deleted in source and destination clusters
- **Scalable:** Creates a configurable set of worker tasks that are distributed across a Kafka
Connect cluster for high performance, even when pushing data over the internet
- **Fault tolerant:** Includes a monitor thread that looks for task failures and optionally auto-restarts
- **Monitoring:** Includes custom JMX metrics for production ready monitoring and alerting

## Overview

Mirus is built around Apache Kafka Connect, providing `SourceConnector` and `SourceTask` implementations
optimized for reading data from Kafka source clusters. The `MirusSourceConnector` runs a
`KafkaMonitor` thread,  which  monitors the source and destination Apache Kafka cluster
partition allocations, looking for changes and applying a configurable topic whitelist.  Each task
is responsible for a subset of the matching partitions, and runs an independent `KafkaConsumer` and
`KafkaProducer` client pair to do the work of replicating those partitions.

Tasks can be restarted independently without otherwise affecting a running cluster, are monitored
continuously for failure, and are optionally automatically restarted.

To understand how Mirus distributes work across a cluser of machines please read the [Kafka Connect documentation](https://kafka.apache.org/documentation/#connect).


## Installation

To build a package containing the Mirus jar file and all dependencies, run `mvn package -P all`:

- `target/mirus-${project.version}-all.zip`

This package can be unzipped for use (see [Quick Start](#quick-start)).

Maven also builds the following artifacts when you run `mvn package`. These are useful if you need
customized packaging for your own environment:

- `target/mirus-${project.version}.jar`: Primary Mirus jar (dependencies not included)
- `target/mirus-${project.version}-run.zip`: A package containing the Mirus run control scripts


## Usage

These instructions assume you have expanded the `mirus-${project.version}-all.zip` package.

### Mirus Worker Instance

A single Mirus Worker can be started using this helper script.

```
> bin/mirus-worker-start.sh [worker-properties-file]
```

`worker-properties-file`: Path to the Mirus `worker properties file`, which configures the Kafka Connect framework. See [quickstart-worker.properties](config/quickstart-worker.properties) for an example.

#### Options:
 `--override property=value`: optional command-line override for any item in the Mirus worker properties file.  Multiple override options are supported (similar to the equivalent flag in Kafka).

### Mirus Offset Tool

Mirus includes a simple tool for reading and writing offsets. This can be useful for migration
from other replication tools, for debugging, and for offset monitoring in production. The tool supports
CSV and JSON input and output.

For detailed usage:

```
> bin/mirus-offset-tool.sh --help
```


## Quick Start

To run the Quick Start example you will need running Kafka and Zookeeper clusters to work with.  We will assume
you have a standard Apache Kafka Quickstart test cluster running on localhost. Follow the [Kafka Quick Start instructions](https://kafka.apache.org/quickstart).

For this tutorial we will set up a Mirus worker instance to mirror the topic `test` in loop-back mode to
another topic in the same cluster. To avoid a conflict the destination topic name will be set to
`test.mirror` using the `destination.topic.name.suffix` configuration option.


1. Build the full Mirus project using Maven

    ```
    > mvn package -P all
    ```

1. Unpack the Mirus "all" package

    ```
    > mkdir quickstart; cd quickstart; unzip ../target/mirus-*-all.zip
    ```

1. Start the `quickstart` worker using the sample worker properties file

    ```
    > bin/mirus-worker-start.sh config/quickstart-worker.properties

    ```

1. In another terminal, confirm the Mirus Kafka Connect REST API is running
    ```
    > curl localhost:8083

    {"version":"1.1.0","commit":"fdcf75ea326b8e07","kafka_cluster_id":"xdxNfx84TU-ennOs7EznZQ"}
    ```

1. Submit a new `MirusSourceConnector` configuration to the REST API with the name `mirus-quickstart-source`

    ```
    > curl localhost:8083/connectors/mirus-quickstart-source/config \
          -X PUT \
          -H 'Content-Type: application/json' \
          -d '{
               "name": "mirus-quickstart-source",
               "connector.class": "com.salesforce.mirus.MirusSourceConnector",
               "tasks.max": "5",
               "topics.whitelist": "test",
               "destination.topic.name.suffix": ".mirror",
               "destination.consumer.bootstrap.servers": "localhost:9092",
               "consumer.bootstrap.servers": "localhost:9092",
               "consumer.client.id": "mirus-quickstart",
               "consumer.key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
               "consumer.value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer"
           }'
    ```
1. Confirm the new connector is running
    ```
    > curl localhost:8083/connectors

    ["mirus-quickstart-source"]
    ```
    ```$bash
    > curl localhost:8083/connectors/mirus-quickstart-source/status

    {"name":"mirus-quickstart-source","connector":{"state":"RUNNING","worker_id":"1.2.3.4:8083"},"tasks":[],"type":"source"}
    ```

1. Create source and destination topics `test` and `test.mirror` in your Kafka cluster
    ```
    > cd ${KAFKA_HOME}
    
    > bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'test' --partitions 1 --replication-factor 1
    Created topic "test".
    
    > bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic 'test.mirror' --partitions 1 --replication-factor 1
    Created topic "test.mirror".
    ```

1. Mirus should detect that the new source and destination topics are available and create a new Mirus Source Task:

    ```
    > curl localhost:8083/connectors/mirus-quickstart-source/status

    {"name":"mirus-quickstart-source","connector":{"state":"RUNNING","worker_id":"10.126.22.44:8083"},"tasks":[{"state":"RUNNING","id":0,"worker_id":"10.126.22.44:8083"}],"type":"source"}
    ```

Any message you write to the topic `test` will now be mirrored to `test.mirror`.


## REST API

See the documentation for [Kafka Connect REST API](https://kafka.apache.org/documentation/#connect_rest).

## Configuration

## Kafka Connect Configuration

Mirus shares most Worker and Source configuration with the Kafka Connect framework. For general
information on configuring the framework see:

- [Kafka Connect Configuration](https://kafka.apache.org/documentation/#connect_configuring)
- [Kafka Connect Worker Configuration](https://kafka.apache.org/documentation/#connectconfigs)


## Mirus Specific Configuration

Mirus-specific configuration properties are documented in these files:

- [Mirus Source Properties](src/main/java/com/salesforce/mirus/config/SourceConfigDefinition.java)
These can be added to the JSON config object posted to the REST API `/config` endpoint to configure a
new MirusSourceConnector instance. In addition, the Kafka Consumer instances
created by Mirus Tasks can be configured using a `consumer.` prefix on the standard
[Kafka Consumer properties](https://kafka.apache.org/documentation/#consumerconfigs). The equivalent
KafkaProducer options are configured in the Mirus Worker Properties file (see below). The
  `destination.consumer.`prefix can be used to override the properties of the KafkaConsumer that connects to
   the destination Kafka cluster.
Unlike the default Kafka Consumer which has `auto.offset.reset` configured to `latest`, Mirus will by default configure consumer to `earliest` to minimize the possible data loss and ensure high-reliability.

- [Mirus Worker Properties](src/main/java/com/salesforce/mirus/config/MirusConfigDefinition.java)
These are Mirus extensions to the Kafka Connect configuration, and should be applied to the
Worker Properties file provided at startup. The Kafka Producer instances created by Kafka Connect
can also be configured using a `producer.` prefix on the standard
[Kafka Producer properties](https://kafka.apache.org/documentation/#producerconfigs).

## Destination Topic Checking

By default, Mirus checks that the destination topic exists in the destination Kafka cluster before
starting to replicate data to it. This feature can be disabled by setting the
[enable.destination.topic.checking](src/main/java/com/salesforce/mirus/config/SourceConfigDefinition.java#L66)
config option to `false`.

As of version 0.2.0, destination topic checking can also support topic re-routing performed by the
[RegexRouter](https://kafka.apache.org/documentation/#connect_transforms) Single-Message Transformation.
No other `Router` Transformations are supported, so destination topic checking must be disabled in order
to use them.

## Metrics

Mirus produces some custom metrics in addition to the standard Kafka Connect metrics.

JMX Queries are as follows

### Latency (MirrorJmxReporter)

```
objectName="mirus:type=MirusSourceConnector,topic=*" attribute="replication-latency-ms-max"
objectName="mirus:type=MirusSourceConnector,topic=*" attribute="replication-latency-ms-min"
objectName="mirus:type=MirusSourceConnector,topic=*" attribute="replication-latency-ms-avg"
objectName="mirus:type=MirusSourceConnector,topic=*" attribute="replication-latency-ms-count"
```

### Destination Information (MissingPartitionsJmxReporter)

```
objectName="mirus:type=mirus" attribute="missing-dest-partitions-count"
```

### Connector Metrics (ConnectorJmxReporter)

```
objectName="mirus:type=connector-metrics,connector=*" attribute="task-failed-restart-attempts-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="connector-failed-restart-attempts-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="failed-task-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="paused-task-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="destroyed-task-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="running-task-count"
objectName="mirus:type=connector-metrics,connector=*" attribute="unassigned-task-count"
```

### Task Metrics (TaskJmxReporter)

```
objectName="mirus:type=connector-task-metrics,connector=*" attribute="task-failed-restart-attempts-count"
```

## Developer Info

To preform a release: `mvn release:prepare release:perform`
GPG Keys may need to be passed to maven with `-Darguments='-Dgpg.passphrase= -Dgpg.keyname=55Z32RD1'`

# Discussion

Questions or comments can also be posted on the Mirus Github issues page.

# Maintainers

[Paul Davidson](http://github.com/pdavidson100) and [contributors](https://github.com/pdavidson/mirus/graphs/contributors).

# Code Style
This project uses the [Google Java Format](https://github.com/google/google-java-format).
