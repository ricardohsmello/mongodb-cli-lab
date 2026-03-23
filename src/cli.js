#!/usr/bin/env node

import process from "node:process";
import { Command, InvalidArgumentError } from "commander";
import {
  interactiveMainMenu,
  runClean,
  runDown,
  runQuickstart,
  runSearchImportDatabases,
  runSearchQuickstart,
  runStatus,
  runSearchStatus,
  runSearchUp,
  runUp
} from "./index.js";

function parsePositiveInteger(label) {
  return (value) => {
    const parsed = Number(value);

    if (!Number.isInteger(parsed) || parsed < 1) {
      throw new InvalidArgumentError(`${label} must be an integer greater than 0.`);
    }

    return parsed;
  };
}

function parsePort(value) {
  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    throw new InvalidArgumentError("Port must be an integer between 1 and 65535.");
  }

  return parsed;
}

function parseCommaSeparatedList(value) {
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function handleAction(action) {
  action().catch((error) => {
    console.error(`\nError: ${error.message}\n`);
    process.exitCode = 1;
  });
}

function addUpOptions(command) {
  return command
    .option("--topology <type>", "cluster topology: standalone, replica-set, or sharded")
    .option("--shards <number>", "number of shards (sharded topology only)", parsePositiveInteger("Shard count"))
    .option("--replicas <number>", "replica set members (replica-set) or members per shard (sharded)", parsePositiveInteger("Replica set members"))
    .option("-m, --mongodb-version <tag>", "MongoDB Docker image tag", String)
    .option("--port <number>", "MongoDB port: standalone/replica-set base port, or mongos port for sharded", parsePort)
    .option("--search", "enable MongoDB Search support (standalone and replica-set only)")
    .option("--sample-databases <names>", "comma-separated sample databases to prepare, or 'all'")
    .option("--search-mongod-port <number>", "host port for the Search mongod node (used by Search flows)", parsePort)
    .option("--search-port <number>", "host port for mongot gRPC (used by Search flows)", parsePort)
    .option("--metrics-port <number>", "host port for mongot metrics (used by Search flows)", parsePort)
    .option("--storage-path <path>", "directory used for generated files and data")
    .option("--force", "replace an existing saved cluster config when it differs");
}

function addSearchUpOptions(command) {
  return command
    .option("--topology <type>", "cluster topology for Search: standalone or replica-set")
    .option("--shards <number>", "number of shards (not supported for Search flows)", parsePositiveInteger("Shard count"))
    .option("--replicas <number>", "replica set members", parsePositiveInteger("Replica set members"))
    .option("-m, --mongodb-version <tag>", "MongoDB Docker image tag for Search flows (8.2)", String)
    .option("--port <number>", "MongoDB port: standalone port or replica-set base port", parsePort)
    .option("--search", "enable MongoDB Search support")
    .option("--sample-databases <names>", "comma-separated sample databases to prepare, or 'all'")
    .option("--search-mongod-port <number>", "host port for the Search mongod node (used by Search flows)", parsePort)
    .option("--search-port <number>", "host port for mongot gRPC (used by Search flows)", parsePort)
    .option("--metrics-port <number>", "host port for mongot metrics (used by Search flows)", parsePort)
    .option("--storage-path <path>", "directory used for generated files and data")
    .option("--force", "replace an existing saved cluster config when it differs");
}

const program = new Command();

program
  .name("mongodb-cli-lab")
  .description("CLI to spin up local MongoDB labs with Docker")
  .version("1.0.1")
  .addHelpText(
    "after",
    `
Examples:
  mongodb-cli-lab
  mongodb-cli-lab up
  mongodb-cli-lab up --topology standalone --port 28000
  mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000
  mongodb-cli-lab up --topology replica-set --replicas 3 --mongodb-version 8.2 --port 28000 --search
  mongodb-cli-lab up --topology sharded --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
  mongodb-cli-lab search up
  mongodb-cli-lab search import --databases sample_airbnb,sample_mflix
  mongodb-cli-lab search quickstart
  mongodb-cli-lab status
  mongodb-cli-lab down
  mongodb-cli-lab clean
  mongodb-cli-lab quickstart
`
  );

addUpOptions(
  program
    .command("up")
    .description("Create and start a local MongoDB cluster: standalone, replica-set, or sharded")
    .addHelpText(
      "after",
      `
Topology notes:
  standalone   Single MongoDB node. Supports --search.
  replica-set  Replica set cluster. Supports --replicas and --search.
  sharded      Sharded cluster. Supports --shards and --replicas.

Restrictions:
  --search is supported only with standalone and replica-set.
  --shards applies only when --topology sharded is used.
`
    )
).action((options) => handleAction(() => runUp(options)));

program
  .command("down")
  .description("Stop the running cluster")
  .action(() => handleAction(runDown));

program
  .command("status")
  .description("Show cluster status")
  .action(() => handleAction(runStatus));

program
  .command("clean")
  .description("Remove containers, volumes, and generated files")
  .action(() => handleAction(runClean));

const searchProgram = program
  .command("search")
  .description("Use MongoDB Search on the current cluster");

addSearchUpOptions(
  searchProgram
    .command("up")
    .description("Set up the current cluster with Search support or start Search services")
    .addHelpText(
      "after",
      `
Search restrictions:
  Supported topologies: standalone and replica-set.
  Supported MongoDB version in this flow: 8.2.
  Sharded clusters are not supported in Search flows.
`
    )
).action((options) => handleAction(() => runSearchUp(options)));

searchProgram
  .command("status")
  .description("Show MongoDB Search status")
  .action(() => handleAction(runSearchStatus));

searchProgram
  .command("import")
  .description("Import sample databases into the Search node")
  .option("--databases <names>", "comma-separated sample databases to import, or 'all'")
  .action((options) =>
    handleAction(() =>
      runSearchImportDatabases({
        all: options.databases === "all",
        databaseNames: options.databases && options.databases !== "all"
          ? parseCommaSeparatedList(options.databases)
          : []
      })
    ));

addSearchUpOptions(
  searchProgram
    .command("quickstart")
    .description("Use the current cluster Search support and run the sample Search flow")
    .addHelpText(
      "after",
      `
Search restrictions:
  Supported topologies: standalone and replica-set.
  Supported MongoDB version in this flow: 8.2.
  Sharded clusters are not supported in Search flows.
`
    )
).action((options) => handleAction(() => runSearchQuickstart(options)));

addUpOptions(
  program
    .command("quickstart")
    .description("Create and start a cluster with default values and no prompts")
    .addHelpText(
      "after",
      `
Quickstart behavior:
  sharded      Creates the cluster and runs the sharding demo.
  standalone   Creates the cluster only, unless --search is enabled.
  replica-set  Creates the cluster only, unless --search is enabled.

Restrictions:
  --search is supported only with standalone and replica-set.
`
    )
).action((options) => handleAction(() => runQuickstart(options)));

program.action(() => handleAction(interactiveMainMenu));

program.parseAsync(process.argv);
