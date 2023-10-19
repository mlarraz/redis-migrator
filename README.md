## About
[![GitHub Super-Linter](https://github.com/opstree/redis-migration/workflows/CI%20Pipeline/badge.svg)](https://github.com/opstree/redis-migration)
[![made-with-Go](https://img.shields.io/badge/Made%20with-Go-1f425f.svg)](http://golang.org)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/opstree/redis-migration)
[![Go Report Card](https://goreportcard.com/badge/github.com/opstree/redis-migration)](https://goreportcard.com/report/github.com/opstree/redis-migration)
[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/opstree/redis-migration)


The Redis Migrator is a Golang-based tool designed to migrate database keys from one Redis cluster to another.

Since Redis `5.0.0`, you can use `REPLICAOF` command to replicate data from one redis to another.

This application is developed for the older version of Redis or AWS ElastiCache instances that do not support `REPLICAOF` command ([AWS docs](https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/RestrictedCommands.html)). 

### Supported key types:
- String
- Hash
- List
- Set
- Sorted Set

## Usage

### Installation

#### Using Go
```shell
go install github.com/huantt/redis-migrator@latest
```

#### Using Docker
```shell
docker run --rm -v /path/to/migrate.yaml:/data/migrate.yaml huanttok/redis-migrator migrate --config.file=/data/migrate.yaml \
--log.level=debug
```

To call `localhost` while using Docker, use `host.docker.internal` instead while using Docker.

### Configuration

For using redis-migrator, we have to create a configuration file and provide some needful information to it. An example configuration file will look like this

```yaml
---
old_redis:
  host: localhost # IP redis server
  port: 6379 # Port redis server
  password: "" # Password of redis server, leave empty if there is no password

new_redis:
  host: localhost
  port: 6380
  password: ""

migration_databases: [0,1,2,3,4,5,6,7,8] # Databases list which needs to be migrated
concurrent_workers: 5
clear_before_migration: true
```

### Available Options

There are help page available for redis-migrator which can be called by help or --help command.

```shell
$ redis-migrator help
A tool for migrating redis database and its keys.

Usage:
  redis-migrator [flags]
  redis-migrator [command]

Available Commands:
  help        Help about any command
  migrate     Runs redis-migrator to run migration
  version     Prints the current version.

Flags:
  -h, --help                help for redis-migrator
      --log.format string   redis-migrator log format. (default "text")
      --log.level string    redis-migrator logging level. (default "info")

Use "redis-migrator [command] --help" for more information about a command.
```

### Using redis-migrator

Simply we have to specify the path of `config.yaml`

```shell
$ redis-migrator migrate --config.file config.yaml --log.level=debug
```
