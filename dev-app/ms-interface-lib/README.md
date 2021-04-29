---
title: "microservice-interface-class-information"
date: 2021-02-08T10:00:00
categories:
  - Readme
tags:
  - library
  - python
  - docker
excerpt: "Summary information of interface classes for container solutions"
---
# Microservice Interface Class Summary
## Overview
Summary information of interface classes for container solutions.

## Class: sftp_CONN
Class wrapper for pysftp library. Implementation of sftp server client interface logic for file access.
| Class      | Methods                     | Description                                       |
|------------|-----------------------------|---------------------------------------------------|
| sftp_CONN  | set_from_env                | Set parameters from environment variables         |
|            | get_dir_list                | Get recursive list of files                       |
|            | open_connection             | Get/Set sftp connection                           |
|            | close_connection            | Closes sftp connection                            |
|            | path_exists                 | Check if path exist on sftp server                |
|            | create_directory            | Recursively create parent directories of a path   |
|            | download_file               |                                                   |
|            | upload_file                 |                                                   |
|            | delete_file                 |                                                   |

## Class: queue_CONN
Class wrapper for pika library. Implementation of RabbitMQ interface logic for intra-container/task messaging.
| Class      | Methods                     | Description                                       |
|------------|-----------------------------|---------------------------------------------------|
| queue_CONN | set_from_env                | Set parameters from environment variables         |
|            | create_namespace_queues     | Initiate queues used to report application status |
|            | create_named_channel_queues | Initiate all standardized queues                  |
|            | start_input_stream          | Start consuming from Input queue                  |
|            | stop_input_stream           | Stop consuming from Input queue                   |
|            | write_to_output             | Publish a message to Output queue                 |
|            | publish_message             | Generically publish a message to a provided queue |
|            | start_consuming             | Generically start consuming from a provided queue |
|            | stop_consuming              | Generically stop consuming from a provided queue  |
|            | write_success               | Publish a message to Success queue                |
|            | write_fault                 | Publish a message to Fault queue                  |
|            | write_status                | Publish a message to Status queue                 |


## Class: db_CONN
Class wrapper for pyodbc library. Implementation of ms-sql-server client interface logic for RDBMS access.
| Class      | Methods                     | Description                                       |
|------------|-----------------------------|---------------------------------------------------|
| db_CONN    |                             |                                                   |
