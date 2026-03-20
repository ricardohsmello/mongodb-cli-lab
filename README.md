# MongoDB CLI Lab

`mongodb-cli-lab` is a Node.js CLI for creating and managing a local MongoDB sharded cluster with Docker.

It is built for tests, local development, demos, and learning. It is not intended for production deployments.

## Features

- Create a local MongoDB sharded cluster
- Configure shard count and replica set members
- Choose the MongoDB version used by the cluster
- Inspect cluster status
- Stop or fully remove the lab environment
- Start with an interactive menu or direct commands
- Run a built-in quickstart demo with sharded sample data

## Prerequisites

Before using this CLI, make sure Docker is installed and running.

## Installation

Install globally:

```bash
npm install -g @ricardohsmello/mongodb-cli-lab
```

Or run it directly with `npx`:

```bash
npx @ricardohsmello/mongodb-cli-lab
```

## Usage

The CLI supports two usage modes.

### Interactive mode

Run the CLI without a command to open the interactive menu:

```bash
mongodb-cli-lab
```

Use this mode when you want to explore the lab step by step.

### Command mode

Run commands directly to create and manage the cluster:

```bash
mongodb-cli-lab up
mongodb-cli-lab status
mongodb-cli-lab down
mongodb-cli-lab clean
mongodb-cli-lab quickstart
```

Use this mode when you want a faster or more reproducible workflow.

## Commands

### `up`

Create and start the cluster.

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

### `status`

Show the current cluster status.

```bash
mongodb-cli-lab status
```

### `down`

Stop the cluster while keeping generated files and data.

```bash
mongodb-cli-lab down
```

### `clean`

Remove containers, volumes, and generated files for the lab.

```bash
mongodb-cli-lab clean
```

### `quickstart`

Create a cluster with default values and automatically build a demo collection.

It:

- starts the cluster
- creates `library.books`
- shards the collection with `{ _id: "hashed" }`
- inserts 500 sample documents
- shows the distribution across shards

```bash
mongodb-cli-lab quickstart
```

## Common Examples

Start the interactive menu:

```bash
mongodb-cli-lab
```

Start a cluster directly:

```bash
mongodb-cli-lab up \
  --shards 2 \
  --replicas 3 \
  --mongodb-version 8.2 \
  --port 28000
```

Stop the cluster:

```bash
mongodb-cli-lab down
```

Delete the cluster and generated files:

```bash
mongodb-cli-lab clean
```

Run the built-in demo:

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

## Notes

- This project is intended for local experimentation
- Docker must be running before executing the CLI
- For a fast first run, use `mongodb-cli-lab quickstart`

