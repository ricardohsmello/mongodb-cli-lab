#!/usr/bin/env node

import process from "node:process";
import { Command, InvalidArgumentError } from "commander";
import {
  interactiveMainMenu,
  runClean,
  runDown,
  runQuickstart,
  runStatus,
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

function handleAction(action) {
  action().catch((error) => {
    console.error(`\nError: ${error.message}\n`);
    process.exitCode = 1;
  });
}

function addUpOptions(command) {
  return command
    .option("--shards <number>", "number of shards", parsePositiveInteger("Shard count"))
    .option("--replicas <number>", "replica set members per shard", parsePositiveInteger("Replica set members"))
    .option("-m, --mongodb-version <tag>", "MongoDB Docker image tag", String)
    .option("--port <number>", "mongos port", parsePort)
    .option("--storage-path <path>", "directory used for generated files and data")
    .option("--force", "replace an existing saved cluster config when it differs");
}

const program = new Command();

program
  .name("mongodb-cli-lab")
  .description("CLI to spin up a local MongoDB sharded cluster with Docker")
  .version("1.0.1")
  .addHelpText(
    "after",
    `
Examples:
  mongodb-cli-lab
  mongodb-cli-lab up
  mongodb-cli-lab up --shards 2 --replicas 3 --mongodb-version 8.2 --port 28000
  mongodb-cli-lab status
  mongodb-cli-lab down
  mongodb-cli-lab clean
  mongodb-cli-lab quickstart
`
  );

addUpOptions(
  program
    .command("up")
    .description("Create and start a local MongoDB sharded cluster")
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

addUpOptions(
  program
    .command("quickstart")
    .description("Create and start a cluster with default values and no prompts")
).action((options) => handleAction(() => runQuickstart(options)));

program.action(() => handleAction(interactiveMainMenu));

program.parseAsync(process.argv);
