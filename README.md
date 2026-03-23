# MongoDB CLI Lab

`mongodb-cli-lab` is a Node.js CLI to create local MongoDB labs with Docker.

NPM package: [@ricardohsmello/mongodb-cli-lab](https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab)

It supports:
- `standalone`
- `replica-set`
- `sharded`
- MongoDB Search on `standalone` and `replica-set`

It is intended for local development, demos, testing, and learning. It is not intended for production use.

## What This CLI Does

With this CLI you can:
- open an interactive menu to create and manage labs
- create a standalone MongoDB node
- create a replica set
- create a sharded cluster
- enable MongoDB Search on standalone or replica-set labs
- run a sharding quickstart for sharded clusters
- run a Search quickstart for standalone or replica-set labs with Search enabled

## Prerequisites

- Docker installed and running
- Node.js installed

For MongoDB Search flows, use MongoDB `8.2`.

## Installation

Install globally:

```bash
npm install -g @ricardohsmello/mongodb-cli-lab
```

Or run from the project source:

```bash
node src/cli.js
```

## Start With The Interactive Menu

To open the interactive menu:

```bash
npx mongodb-cli-lab
```

Or, if you are running from source:

```bash
node src/cli.js
```

This opens the main interactive menu, where you can:
- set up a cluster
- open the Search lab
- work with data and sharding
- manage the cluster

## Ready-To-Run Commands

These commands run directly without opening the full interactive menu.

### Main Commands

```bash
node src/cli.js up
node src/cli.js status
node src/cli.js down
node src/cli.js clean
node src/cli.js quickstart
```

### Search Commands

```bash
node src/cli.js search up
node src/cli.js search status
node src/cli.js search import --databases sample_mflix
node src/cli.js search quickstart
```

### Commands You Will Probably Use Most

Open the interactive menu:

```bash
node src/cli.js
```

Run a sharded cluster quickstart:

```bash
node src/cli.js quickstart --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

Run a replica set with Search quickstart:

```bash
node src/cli.js quickstart --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000
```

Start Search services or Search flow directly:

```bash
node src/cli.js search up
node src/cli.js search quickstart
```

## Full Command-Line Options

The main cluster creation command is:

```bash
mongodb-cli-lab up [options]
```

Available options:

```bash
--topology <type>
--shards <number>
--replicas <number>
-m, --mongodb-version <tag>
--port <number>
--search
--sample-databases <names>
--search-mongod-port <number>
--search-port <number>
--metrics-port <number>
--storage-path <path>
--force
```

### Important Rules

- `--topology` can be `standalone`, `replica-set`, or `sharded`
- `--shards` is only for `sharded`
- `--replicas` means:
  - number of members in `replica-set`
  - number of members per shard in `sharded`
- `--search` works only with `standalone` and `replica-set`
- Search flows should use MongoDB `8.2`
- `--sample-databases <names>` accepts comma-separated names or `all`

## Examples By Topology

### Standalone

Standalone without Search:

```bash
mongodb-cli-lab up --topology standalone --mongodb-version 8.2 --port 28000
```

Standalone with Search:

```bash
mongodb-cli-lab up --topology standalone --search --mongodb-version 8.2 --port 28000
```

### Replica Set

Replica set without Search:

```bash
mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000
```

Replica set with Search:

```bash
mongodb-cli-lab up --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000
```

Replica set with Search quickstart:

```bash
mongodb-cli-lab quickstart --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000
```

### Sharded Cluster

Sharded cluster:

```bash
mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

Sharded cluster with sample databases:

```bash
mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000 --sample-databases all
```

Sharded cluster quickstart:

```bash
mongodb-cli-lab quickstart --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

## Search Commands

Search commands are grouped under:

```bash
mongodb-cli-lab search
```

Available commands:

```bash
mongodb-cli-lab search up
mongodb-cli-lab search status
mongodb-cli-lab search import --databases sample_airbnb,sample_mflix
mongodb-cli-lab search quickstart
```

### Search Restrictions

- Search works only with `standalone` and `replica-set`
- Search flows should use MongoDB `8.2`
- Search is not supported on `sharded` in this CLI

### Search Quickstart

This command:

```bash
mongodb-cli-lab search quickstart
```

does the following:
- ensures Search is enabled
- restores `sample_mflix`
- creates the `default` Search index on `sample_mflix.movies`
- runs a sample `$search` query for `"baseball"`

## Sharding Quickstart

This command:

```bash
mongodb-cli-lab quickstart --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

does the following:
- creates a sharded cluster
- creates the demo collection `library.books`
- shards the collection using `{ "_id": "hashed" }`
- inserts sample documents
- shows distribution across shards

## Lifecycle Commands

```bash
mongodb-cli-lab status
mongodb-cli-lab down
mongodb-cli-lab clean
```

## Help

To inspect the CLI help:

```bash
mongodb-cli-lab --help
mongodb-cli-lab up --help
mongodb-cli-lab quickstart --help
mongodb-cli-lab search --help
mongodb-cli-lab search up --help
mongodb-cli-lab search quickstart --help
```
