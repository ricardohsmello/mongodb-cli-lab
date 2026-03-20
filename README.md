# MongoDB CLI Lab

`mongodb-cli-lab` is a CLI for creating and managing a local MongoDB sharded cluster with Docker.

It is intended for tests, local development, demos, and learning. It is not a production deployment tool.

## Installation

```bash
npm install -g @ricardohsmello/mongodb-cli-lab
```

Or run it directly with `npx`:

```bash
npx @ricardohsmello/mongodb-cli-lab
```

## Two Ways To Use It

### 1. Interactive mode

Run the CLI with no command to open the interactive menu:

```bash
mongodb-cli-lab
```

This mode is useful when you want to explore the lab step by step.

### 2. Command mode

Run commands directly to create and manage the cluster without going through the menu.

Available commands:

```bash
mongodb-cli-lab up
mongodb-cli-lab status
mongodb-cli-lab down
mongodb-cli-lab clean
mongodb-cli-lab quickstart
```

## Start A Cluster With Commands

Example:

```bash
mongodb-cli-lab up \
  --shards 2 \
  --replicas 3 \
  --mongodb-version 8.2 \
  --port 28000
```

This starts a cluster with:

- 2 shards
- 3 replica set members per shard
- MongoDB `8.2`
- `mongos` exposed on port `28000`

## Quickstart

`quickstart` creates a cluster with default values and also builds a small sharding demo automatically.

It:

- starts the cluster
- creates `library.books`
- shards the collection with `{ _id: "hashed" }`
- inserts 500 sample documents
- shows distribution across shards

Run:

```bash
mongodb-cli-lab quickstart
```

## Local Development

If you are running the project from source:

```bash
node src/cli.js
```

Example:

```bash
node src/cli.js up \
  --shards 2 \
  --replicas 3 \
  --mongodb-version 8.2 \
  --port 28000
```

Other commands:

```bash
node src/cli.js status
node src/cli.js down
node src/cli.js clean
node src/cli.js quickstart
```

## Command Summary

- `up`: create and start the cluster
- `status`: show current cluster status
- `down`: stop the cluster
- `clean`: remove containers, volumes, and generated files
- `quickstart`: start the cluster and build a ready-to-use sharding demo

