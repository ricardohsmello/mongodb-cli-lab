import path from "node:path";
import process from "node:process";
import inquirer from "inquirer";

const DEFAULT_STORAGE_PATH = "./mongodb-cli-lab";
const CONFIG_SERVER_MEMBERS = 3;

export const DEFAULT_UP_OPTIONS = Object.freeze({
  shards: 2,
  replicas: 3,
  mongodbVersion: "8.2",
  port: 28000,
  storagePath: DEFAULT_STORAGE_PATH
});

function validatePositiveInteger(label) {
  return (value) => {
    if (!Number.isInteger(value) || value < 1) {
      return `${label} must be an integer greater than 0.`;
    }

    return true;
  };
}

function validatePort(value) {
  if (!Number.isInteger(value) || value < 1 || value > 65535) {
    return "Port must be an integer between 1 and 65535.";
  }

  return true;
}

function normalizeClusterConfig(config) {
  return {
    shardCount: config.shardCount,
    replicaSetMembers: config.replicaSetMembers,
    mongodbVersion: config.mongodbVersion,
    mongosPort: config.mongosPort,
    storagePath: path.resolve(process.cwd(), config.storagePath),
    configServerReplicaSet: "configReplSet",
    configServerMembers: CONFIG_SERVER_MEMBERS
  };
}

function hasValue(value) {
  return value !== undefined && value !== null;
}

export function hasExplicitUpOptions(options = {}) {
  return ["shards", "replicas", "mongodbVersion", "port", "storagePath"].some((key) => hasValue(options[key]));
}

export function buildQuickstartConfig(overrides = {}) {
  return normalizeClusterConfig({
    shardCount: overrides.shards ?? DEFAULT_UP_OPTIONS.shards,
    replicaSetMembers: overrides.replicas ?? DEFAULT_UP_OPTIONS.replicas,
    mongodbVersion: overrides.mongodbVersion ?? DEFAULT_UP_OPTIONS.mongodbVersion,
    mongosPort: overrides.port ?? DEFAULT_UP_OPTIONS.port,
    storagePath: overrides.storagePath ?? DEFAULT_UP_OPTIONS.storagePath
  });
}

export function configsMatch(left, right) {
  return (
    left.shardCount === right.shardCount &&
    left.replicaSetMembers === right.replicaSetMembers &&
    left.mongodbVersion === right.mongodbVersion &&
    left.mongosPort === right.mongosPort &&
    path.resolve(left.storagePath) === path.resolve(right.storagePath)
  );
}

async function promptForMissingValues(partialConfig) {
  console.log("\nMongoDB CLI Lab Setup\n");

  const questions = [];

  if (!hasValue(partialConfig.shardCount)) {
    questions.push({
      type: "list",
      name: "shardCount",
      message: "How many shards do you want?",
      choices: [
        { name: "1 shard", value: 1 },
        { name: "2 shards", value: 2 },
        { name: "3 shards", value: 3 },
        { name: "4 shards", value: 4 },
        { name: "Custom", value: "custom" }
      ],
      default: DEFAULT_UP_OPTIONS.shards
    });
    questions.push({
      type: "input",
      name: "customShardCount",
      message: "Enter the number of shards:",
      when: (answers) => answers.shardCount === "custom",
      validate: (value) => validatePositiveInteger("Shard count")(Number(value))
    });
  }

  if (!hasValue(partialConfig.replicaSetMembers)) {
    questions.push({
      type: "list",
      name: "replicaSetMembers",
      message: "How many replica set members per shard?",
      choices: [
        { name: "1 member", value: 1 },
        { name: "3 members", value: 3 },
        { name: "5 members", value: 5 },
        { name: "Custom", value: "custom" }
      ],
      default: DEFAULT_UP_OPTIONS.replicas
    });
    questions.push({
      type: "input",
      name: "customReplicaSetMembers",
      message: "Enter replica set members per shard:",
      when: (answers) => answers.replicaSetMembers === "custom",
      validate: (value) => validatePositiveInteger("Replica set members")(Number(value))
    });
  }

  if (!hasValue(partialConfig.mongodbVersion)) {
    questions.push({
      type: "list",
      name: "mongodbVersion",
      message: "Which MongoDB version should be used?",
      choices: ["8.2", "8.0", "7.0", "6.0", "5.0", "Custom"],
      default: DEFAULT_UP_OPTIONS.mongodbVersion
    });
    questions.push({
      type: "input",
      name: "customMongodbVersion",
      message: "Enter the MongoDB Docker tag/version:",
      when: (answers) => answers.mongodbVersion === "Custom",
      default: DEFAULT_UP_OPTIONS.mongodbVersion,
      validate: (value) => (value.trim() ? true : "Version cannot be empty.")
    });
  }

  if (!hasValue(partialConfig.mongosPort)) {
    questions.push({
      type: "list",
      name: "mongosPort",
      message: "Which port should mongos expose?",
      choices: [
        { name: "27017", value: 27017 },
        { name: "28000", value: 28000 },
        { name: "30000", value: 30000 },
        { name: "Custom", value: "custom" }
      ],
      default: DEFAULT_UP_OPTIONS.port
    });
    questions.push({
      type: "input",
      name: "customMongosPort",
      message: "Enter the mongos port:",
      when: (answers) => answers.mongosPort === "custom",
      validate: (value) => validatePort(Number(value))
    });
  }

  if (!questions.length) {
    return {};
  }

  const answers = await inquirer.prompt(questions);
  return {
    shardCount:
      answers.shardCount === "custom" ? Number(answers.customShardCount) : answers.shardCount,
    replicaSetMembers:
      answers.replicaSetMembers === "custom"
        ? Number(answers.customReplicaSetMembers)
        : answers.replicaSetMembers,
    mongodbVersion:
      answers.mongodbVersion === "Custom"
        ? answers.customMongodbVersion.trim()
        : answers.mongodbVersion,
    mongosPort:
      answers.mongosPort === "custom" ? Number(answers.customMongosPort) : answers.mongosPort
  };
}

export async function resolveUpConfig(options = {}) {
  if (options.quickstart) {
    return buildQuickstartConfig(options);
  }

  const partialConfig = {
    shardCount: options.shards,
    replicaSetMembers: options.replicas,
    mongodbVersion: options.mongodbVersion,
    mongosPort: options.port,
    storagePath: options.storagePath ?? DEFAULT_UP_OPTIONS.storagePath
  };

  const promptedValues = await promptForMissingValues(partialConfig);

  return normalizeClusterConfig({
    shardCount: promptedValues.shardCount ?? partialConfig.shardCount,
    replicaSetMembers: promptedValues.replicaSetMembers ?? partialConfig.replicaSetMembers,
    mongodbVersion: promptedValues.mongodbVersion ?? partialConfig.mongodbVersion,
    mongosPort: promptedValues.mongosPort ?? partialConfig.mongosPort,
    storagePath: partialConfig.storagePath
  });
}
