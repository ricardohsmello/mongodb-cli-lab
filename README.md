# mongodb-cli-lab

[![npm version](https://img.shields.io/npm/v/@ricardohsmello/mongodb-cli-lab)](https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab)
[![npm downloads](https://img.shields.io/npm/dm/@ricardohsmello/mongodb-cli-lab)](https://www.npmjs.com/package/@ricardohsmello/mongodb-cli-lab)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> A Node.js CLI to spin up local MongoDB labs with Docker — standalone, replica set, sharded cluster, and MongoDB Search.

**⚠️ Disclaimer**

This is an independent project and is **not an official MongoDB product**. It is intended for local development, demos, testing, and learning purposes only.

**⚠️ Do not use this tool in production environments.**
---

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
  - [Interactive Menu](#interactive-menu)
  - [Commands](#commands)
  - [Options](#options)
- [Examples](#examples)
  - [Standalone](#standalone)
  - [Replica Set](#replica-set)
  - [Sharded Cluster](#sharded-cluster)
- [Help](#help)

---

## Features

- Interactive menu to create and manage labs
- Standalone MongoDB node
- Replica set
- Sharded cluster
- MongoDB Search on `standalone` and `replica-set`
- Quickstart scripts for sharding and Search flows
- Sample database loading (`--sample-databases`)

---

## Prerequisites

- [Docker](https://www.docker.com/) installed and running

---

## Installation

Install globally via npm:

```bash
npm install -g @ricardohsmello/mongodb-cli-lab
```

Or run directly from source:

```bash
node src/cli.js
```

---

## Usage

### Interactive Menu

Launch the interactive menu to set up and manage your lab:

```bash
mongodb-cli-lab
```

From the menu you can:
- Set up a cluster (standalone, replica set, or sharded)
- Open the Search lab
- Work with data and sharding
- Manage the cluster lifecycle

### Commands

| Command                    | Description                                      |
|---------------------------|--------------------------------------------------|
| `mongodb-cli-lab`         | Open the interactive menu                        |
| `mongodb-cli-lab up`      | Start a lab with the given topology and options  |
| `mongodb-cli-lab status`  | Show the status of the running lab               |
| `mongodb-cli-lab down`    | Stop the running lab                             |
| `mongodb-cli-lab clean`   | Remove all lab containers and volumes            |
| `mongodb-cli-lab quickstart` | Run a quickstart script for the given topology |

### Options

| Flag                          | Values                                      | Description                                              |
|-------------------------------|---------------------------------------------|----------------------------------------------------------|
| `--topology`                  | `standalone`, `replica-set`, `sharded`      | MongoDB topology to create                               |
| `--mongodb-version`           | e.g. `8.2`                                  | MongoDB Docker image version                             |
| `--port`                      | e.g. `28000`                                | Host port to expose                                      |
| `--replicas`                  | integer                                     | Members in `replica-set`; members per shard in `sharded` |
| `--shards`                    | integer                                     | Number of shards (only for `sharded`)                    |
| `--search`                    | —                                           | Enable MongoDB Search (only for `standalone`/`replica-set`) |
| `--sample-databases`          | comma-separated names or `all`              | Load sample databases after setup                        |

---

## Examples

### Standalone

Without Search:

```bash
mongodb-cli-lab up --topology standalone --mongodb-version 8.2 --port 28000
```

With Search:

```bash
mongodb-cli-lab up --topology standalone --search --mongodb-version 8.2 --port 28000
```

### Replica Set

Without Search:

```bash
mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000
```

With Search:

```bash
mongodb-cli-lab up --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000
```

Search quickstart:

```bash
mongodb-cli-lab quickstart --topology replica-set --replicas 3 --search --mongodb-version 8.2 --port 28000
```

### Sharded Cluster

Basic:

```bash
mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

With sample databases:

```bash
mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000 --sample-databases all
```

Sharding quickstart:

```bash
mongodb-cli-lab quickstart --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
```

> **Note:** MongoDB Search is only supported on `standalone` and `replica-set` topologies.

---

## Help

```bash
mongodb-cli-lab --help
mongodb-cli-lab up --help
mongodb-cli-lab quickstart --help
mongodb-cli-lab search --help
mongodb-cli-lab search up --help
mongodb-cli-lab search quickstart --help
```
