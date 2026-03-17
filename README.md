# MongoDB CLI Lab

`MongoDB CLI Lab` is an npm CLI for learning and testing MongoDB sharding in a local Docker environment.

It helps you create a sharded cluster automatically so you can focus on understanding the workflow instead of setting everything up by hand.

With the CLI, you can:

- create a local MongoDB sharded cluster
- choose the number of shards
- choose the number of replica set members per shard
- choose the MongoDB version
- define the `mongos` port
- create collections
- enable sharding for collections
- run simple experiments to understand shard distribution

This project is intended for study, demos, and local experiments. It is a learning lab, not a production-ready solution.

## Prerequisites

Before running the CLI, you need:

- Docker installed
- Docker running

## How to run

1. Install the package:

```bash
npm i @ricardohsmello/mongodb-cli-lab
```

2. Run the CLI:

```bash
npx mongodb-cli-lab
```

The command opens an interactive flow where you can create the cluster, configure shards, and work with sharded collections.

## Important

This tool was built for educational purposes only.

It should not be used in production as-is. Any production usage would require a proper technical review, operational hardening, and validation by whoever decides to adopt it.
