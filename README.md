# MongoDB CLI Lab

`mongodb-cli-lab` is a Node.js CLI for creating and managing local MongoDB labs with Docker.

It is designed for tests, local development, demos, and learning. It is not intended for production deployments.

## Features

- Start a local MongoDB sharded cluster
- Run a guided sharding quickstart with sample data
- Create a local MongoDB Search lab with `mongod` + `mongot`
- Run a Search quickstart that restores sample data, creates a Search index, and executes a sample `$search` query
- Manage lab lifecycle with simple commands

## Prerequisites

Before using this CLI, make sure Docker is installed and running.

MongoDB Search quickstart uses the MongoDB Community Search image and is intended for MongoDB 8.2+ compatible setups.

## Installation

Install globally:

```bash
npm install -g @ricardohsmello/mongodb-cli-lab
```

Or run it directly with `npx`:

```bash
npx @ricardohsmello/mongodb-cli-lab
```

## Usage Modes

### Interactive mode

Run the CLI without a command to open the interactive menu:

```bash
mongodb-cli-lab
```

The current interactive menu is focused on the sharded cluster lab.

### Command mode

Run commands directly for faster and more reproducible workflows:

```bash
mongodb-cli-lab up
mongodb-cli-lab status
mongodb-cli-lab down
mongodb-cli-lab clean
mongodb-cli-lab quickstart
mongodb-cli-lab search up
mongodb-cli-lab search status
mongodb-cli-lab search down
mongodb-cli-lab search clean
mongodb-cli-lab search quickstart
```

## Sharded Cluster Lab

Start a sharded cluster directly:

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

Lifecycle commands:

```bash
mongodb-cli-lab status
mongodb-cli-lab down
mongodb-cli-lab clean
```

Sharding quickstart:

```bash
mongodb-cli-lab quickstart
```

This quickstart:

- creates the cluster with default values
- creates `library.books`
- shards the collection with `{ _id: "hashed" }`
- inserts 500 sample documents
- shows distribution across shards

## MongoDB Search Lab

Start the Search lab:

```bash
mongodb-cli-lab search up
```

Lifecycle commands:

```bash
mongodb-cli-lab search status
mongodb-cli-lab search down
mongodb-cli-lab search clean
```

Search quickstart:

```bash
mongodb-cli-lab search quickstart
```

This quickstart:

- pulls the MongoDB Community Server and Community Search images
- downloads the sample archive
- creates a local Search lab with `mongod` and `mongot`
- restores sample data
- creates the `default` Search index on `sample_mflix.movies`
- runs a sample `$search` query for `"baseball"`

## Local Development

If you are running the project from source:

```bash
node src/cli.js
```

Sharded cluster example:

```bash
node src/cli.js up \
  --shards 2 \
  --replicas 3 \
  --mongodb-version 8.2 \
  --port 28000
```

Search lab example:

```bash
node src/cli.js search quickstart
```

