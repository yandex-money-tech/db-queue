[![Build Status](https://travis-ci.com/yoomoney-tech/db-queue.svg?branch=master)](https://travis-ci.com/github/yoomoney-tech/db-queue/branches)
[![Codecov](https://codecov.io/gh/yoomoney-tech/db-queue/branch/master/graph/badge.svg)](https://codecov.io/gh/yoomoney-tech/db-queue)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Javadoc](https://img.shields.io/badge/javadoc-latest-blue.svg)](https://yoomoney-tech.github.io/db-queue/)
[![Download](https://img.shields.io/badge/Download-latest)](https://search.maven.org/artifact/ru.yoomoney.tech/db-queue)
# Database Queue

Library provides worker-queue implementation on top of Java and database.  
Project uses [Semantic Versioning](http://semver.org/).  
Library is available on [Maven Central](https://search.maven.org/) 

**Gradle**
```
implementation 'ru.yoomoney.tech:db-queue:13.0.0'
```

**Maven**
```
<dependency>
  <groupId>ru.yoomoney.tech</groupId>
  <artifactId>db-queue</artifactId>
  <version>12.0.0</version>
</dependency>
```

## Why?

There are several reasons:

* You need simple, efficient and flexible task processing tool which supports delayed job execution.
* You already have a database and don't want to introduce additional tools 
in your infrastructure (for example ActiveMq or RabbitMq) 
* You have somewhat small load. This worker queue mechanism can handle 
more than 1000 rps on single database and table. Moreover you can shard your database and get horizontal scalability. 
However we cannot guarantee that it would be easy to auto scale or handle more than 1000 rps.
* You require strong guaranties for task delivery or processing. Library offers at-least-once delivery semantic. 

## How it works?

1. You have a task that you want to process later. 
2. You tell [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) to schedule the task. 
3. [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) optionally chooses a database shard.
4. [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) converts the task payload to string representation through [TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java). 
5. [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) inserts the task in the database through [QueueDao](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/dao/QueueDao.java).
6. ... the task has been selected from database at specified time according to queue settings ... 
7. The task payload is converted to typed representation through [TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java).
8. The task is passed to the [QueueConsumer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueConsumer.java) instance in order to be processed. 
9. You process the task and return processing result. 
10. ... the task is updated according to processing result and queue settings ...

## Features

* Persistent working-queue
* Support for PostgreSQL, MSSQL, Oracle, H2.
* Storing queue tasks in a separate tables or in the same table ([QueueLocation](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueLocation.java)).
* Storing queue tasks in a separate databases for horizontal scaling ([QueueShard](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/QueueShard.java)).
* Delayed task execution.
* At-least-once task processing semantic.
* Several retry strategies in case of a task processing error ([TaskRetryType](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/TaskRetryType.java)).
* Task event listeners ([TaskLifecycleListener](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/TaskLifecycleListener.java), [ThreadLifecycleListener](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/ThreadLifecycleListener.java)).
* Strong-typed api for task processing and enqueuing ([TaskPayloadTransformer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/TaskPayloadTransformer.java)).
* Several task processing modes ([ProcessingMode](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/ProcessingMode.java)).
* And many other features, look at [Settings package](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings). 

## Database support

As of now the library supports PostgreSQL, MSSQL and Oracle as backing database, however library architecture
makes it easy to add other relational databases which has support for transactions and "for update skip locked" feature,  
for example MySql.  
Feel free to add support for other databases via pull request.

## Dependencies

Library contains minimal set of dependencies to easily integrate in any project:

* `db-queue-core` module requires only`org.slf4j:slf4j-api` library
* `db-queue-spring` module requires Spring Framework (spring-jdbc and spring-tx) for interacting with a database. 
Other features of Spring ecosystem are not in use. 

# Usage

## Configuration

### PostgreSQL

Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        TEXT NOT NULL,
  payload           TEXT,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks USING btree (queue_name, next_process_at, id DESC);
```

You should always analyze your database workload before applying 
these recommendations. Settings heavily depends on a hardware, and a load you have.

* Fill Factor 

You need to set a low fill-factor for table in order to 
let database put row updates to the same page.
In that case database will need less amount of random page writes. 
This technique also prevents fragmentation so we get more robust selects. 
Same rules are applied to an indexes. You can safely set fill-factor 
for tables and indexes to 30%.

Our production settings for frequently updated tasks table are:
```sql
CREATE TABLE queue_tasks (...) WITH (fillfactor=30)
CREATE INDEX ... ON queue_tasks USING btree (...) WITH (fillfactor=30)
``` 

* Autovacuum

You need to make autovacuum more aggressive in order to eliminate dead tuples. 
Dead tuples leads to excessive page reads because they occupy space 
that can be reused by active tuples. Autovacuum can be configured in many ways, 
for example, you can set 
[scale-factor](https://www.postgresql.org/docs/current/static/runtime-config-autovacuum.html#GUC-AUTOVACUUM-VACUUM-SCALE-FACTOR) to 1% or even lower.

Our production settings for frequently updated tasks tables are:
```sql
CREATE TABLE queue_tasks (...) WITH (
autovacuum_vacuum_cost_delay=5, 
autovacuum_vacuum_cost_limit=500,
autovacuum_vacuum_scale_factor=0.0001)
```

### MSSQL

Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                INT IDENTITY(1,1) NOT NULL,
  queue_name        TEXT NOT NULL,
  payload           TEXT,
  created_at        DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  next_process_at   DATETIMEOFFSET NOT NULL  DEFAULT SYSDATETIMEOFFSET(),
  attempt           INTEGER NOT NULL         DEFAULT 0,
  reenqueue_attempt INTEGER NOT NULL         DEFAULT 0,
  total_attempt     INTEGER NOT NULL         DEFAULT 0,
  PRIMARY KEY (id)
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```

### Oracle

Create table (with index) where tasks will be stored.
```sql
CREATE TABLE queue_tasks (
  id                NUMBER(38) NOT NULL PRIMARY KEY,
  queue_name        VARCHAR2(128) NOT NULL,
  payload           CLOB,
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  attempt           NUMBER(38)                  DEFAULT 0,
  reenqueue_attempt NUMBER(38)                  DEFAULT 0,
  total_attempt     NUMBER(38)                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```
Create sequence and specify its name through `QueueLocation.Builder.withIdSequence(String)` 
or `id-sequence` in file config.
```sql
CREATE SEQUENCE tasks_seq;
```

### H2 database

A table that is needed for a work 
```sql
CREATE TABLE queue_tasks (
  id                BIGSERIAL PRIMARY KEY,
  queue_name        VARCHAR(100) NOT NULL,
  payload           VARCHAR(100),
  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),
  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),
  attempt           INTEGER                  DEFAULT 0,
  reenqueue_attempt INTEGER                  DEFAULT 0,
  total_attempt     INTEGER                  DEFAULT 0
);
CREATE INDEX queue_tasks_name_time_desc_idx
  ON queue_tasks (queue_name, next_process_at, id DESC);
```

### Code

Example configuration is shown in [ExampleConfiguration](db-queue-spring//src/test/java/ru/yoomoney/tech/dbqueue/example/ExampleConfiguration.java).

The main steps are:
* Specify a queue configuration through [QueueConfig](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueConfig.java) instance (or use [QueueConfigsReader](src/main/java/ru/yoomoney/tech/dbqueue/settings/QueueConfigsReader.java)).
* Implement [QueueProducer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueProducer.java) interface.
* Implement [QueueConsumer](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api/QueueConsumer.java) interface.
* Create [QueueService](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config/QueueService.java).
* Register `QueueConsumer` in `QueueService`
* Start queues through `QueueService`

## Project structure

* internal

Internal classes. **Not for public use**.

*Backward compatibility for classes in that package maybe broken in any release*

* [api](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/api)

You should provide implementation for interfaces in that package.
The package contains classes which are involved in processing or enqueueing tasks.

* [settings](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/settings)

Queue settings.

* [dao](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/dao)

Additional classes for managing storage.

* [config](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/config)

Registration and configuration.

# Known Issues

* Uneven load balancing

One of the hosts can consequently process several tasks very quickly while other hosts are sleeping.

* No support for Blue-green deployment

There is no support for blue-green deployment because a task is not bound to a host or to a group of hosts. 

* Hard to write tests.

Task processing is asynchronous. Therefore, it is hard to write tests because you always must think about that fact
and write code according to it. To ease development of tests you can use `wakeup` method of [QueueService](db-queue-core/src/main/java/ru/yoomoney/tech/dbqueue/init/QueueService.java)
