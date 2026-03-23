import path from "node:path";
import process from "node:process";
import inquirer from "inquirer";

const DEFAULT_STORAGE_PATH = "./mongodb-cli-lab";
const CONFIG_SERVER_MEMBERS = 3;
const DEFAULT_SEARCH_MONGOD_PORT = 27018;
const DEFAULT_SEARCH_PORT = 27028;
const DEFAULT_SEARCH_METRICS_PORT = 9946;
const AVAILABLE_SAMPLE_DATABASES = Object.freeze([
  "sample_airbnb",
  "sample_analytics",
  "sample_geospatial",
  "sample_guides",
  "sample_mflix",
  "sample_restaurants",
  "sample_supplies",
  "sample_training",
  "sample_weatherdata"
]);

export const DEFAULT_UP_OPTIONS = Object.freeze({
  topology: "standalone",
  shards: 2,
  replicas: 3,
  mongodbVersion: "8.2",
  port: 28000,
  storagePath: DEFAULT_STORAGE_PATH,
  search: false,
  sampleDatabases: [],
  searchMongodPort: DEFAULT_SEARCH_MONGOD_PORT,
  searchPort: DEFAULT_SEARCH_PORT,
  metricsPort: DEFAULT_SEARCH_METRICS_PORT
});

function normalizeSampleDatabases(value) {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return [...new Set(value.filter(Boolean))];
  }

  if (typeof value === "string") {
    if (value === "all") {
      return [...AVAILABLE_SAMPLE_DATABASES];
    }

    return [...new Set(value.split(",").map((entry) => entry.trim()).filter(Boolean))];
  }

  return [];
}

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

function getChoiceDefaultIndex(choices, value) {
  const index = choices.findIndex((choice) =>
    typeof choice === "object" ? choice.value === value : choice === value
  );

  return index >= 0 ? index : 0;
}

function normalizeClusterConfig(config) {
  const topology = config.topology ?? DEFAULT_UP_OPTIONS.topology;
  const shardCount = topology === "sharded" ? (config.shardCount ?? DEFAULT_UP_OPTIONS.shards) : 0;
  const replicaSetMembers = topology === "standalone"
    ? 1
    : (config.replicaSetMembers ?? DEFAULT_UP_OPTIONS.replicas);

  return {
    topology,
    shardCount,
    replicaSetMembers,
    mongodbVersion: config.mongodbVersion,
    mongosPort: config.mongosPort,
    storagePath: path.resolve(process.cwd(), config.storagePath),
    features: {
      search: Boolean(config.search)
    },
    sampleDatabases: normalizeSampleDatabases(config.sampleDatabases),
    search: {
      mongodPort: config.searchMongodPort ?? DEFAULT_SEARCH_MONGOD_PORT,
      mongotPort: config.searchPort ?? DEFAULT_SEARCH_PORT,
      metricsPort: config.metricsPort ?? DEFAULT_SEARCH_METRICS_PORT
    },
    configServerReplicaSet: "configReplSet",
    configServerMembers: CONFIG_SERVER_MEMBERS
  };
}

function hasValue(value) {
  return value !== undefined && value !== null;
}

export function hasExplicitUpOptions(options = {}) {
  return [
    "topology",
    "shards",
    "replicas",
    "mongodbVersion",
    "port",
    "storagePath",
    "search",
    "sampleDatabases",
    "searchMongodPort",
    "searchPort",
    "metricsPort"
  ].some((key) => hasValue(options[key]));
}

export function buildQuickstartConfig(overrides = {}) {
  return normalizeClusterConfig({
    topology: overrides.topology ?? DEFAULT_UP_OPTIONS.topology,
    shardCount: overrides.shards ?? DEFAULT_UP_OPTIONS.shards,
    replicaSetMembers: overrides.replicas ?? DEFAULT_UP_OPTIONS.replicas,
    mongodbVersion: overrides.mongodbVersion ?? DEFAULT_UP_OPTIONS.mongodbVersion,
    mongosPort: overrides.port ?? DEFAULT_UP_OPTIONS.port,
    storagePath: overrides.storagePath ?? DEFAULT_UP_OPTIONS.storagePath,
    search: overrides.search ?? DEFAULT_UP_OPTIONS.search,
    sampleDatabases: overrides.sampleDatabases ?? DEFAULT_UP_OPTIONS.sampleDatabases,
    searchMongodPort: overrides.searchMongodPort ?? DEFAULT_UP_OPTIONS.searchMongodPort,
    searchPort: overrides.searchPort ?? DEFAULT_UP_OPTIONS.searchPort,
    metricsPort: overrides.metricsPort ?? DEFAULT_UP_OPTIONS.metricsPort
  });
}

export function configsMatch(left, right) {
  return (
    left.topology === right.topology &&
    left.shardCount === right.shardCount &&
    left.replicaSetMembers === right.replicaSetMembers &&
    left.mongodbVersion === right.mongodbVersion &&
    left.mongosPort === right.mongosPort &&
    Boolean(left.features?.search) === Boolean(right.features?.search) &&
    (left.search?.mongodPort ?? DEFAULT_SEARCH_MONGOD_PORT) === (right.search?.mongodPort ?? DEFAULT_SEARCH_MONGOD_PORT) &&
    (left.search?.mongotPort ?? DEFAULT_SEARCH_PORT) === (right.search?.mongotPort ?? DEFAULT_SEARCH_PORT) &&
    (left.search?.metricsPort ?? DEFAULT_SEARCH_METRICS_PORT) === (right.search?.metricsPort ?? DEFAULT_SEARCH_METRICS_PORT) &&
    JSON.stringify(left.sampleDatabases ?? []) === JSON.stringify(right.sampleDatabases ?? []) &&
    path.resolve(left.storagePath) === path.resolve(right.storagePath)
  );
}

function supportsSearch(version) {
  const match = String(version ?? "").match(/^(\d+)(?:\.(\d+))?/);
  if (!match) {
    return false;
  }

  const major = Number(match[1]);
  const minor = Number(match[2] ?? 0);
  return major > 8 || (major === 8 && minor >= 2);
}

function supportsSearchTopology(topology) {
  return topology === "standalone" || topology === "replica-set";
}

function resolvePromptValue(promptedValues, partialConfig, key, fallback) {
  if (hasValue(promptedValues[key])) {
    return promptedValues[key];
  }

  if (hasValue(partialConfig[key])) {
    return partialConfig[key];
  }

  return fallback;
}

async function promptForMissingValues(partialConfig) {
  console.log("\nMongoDB CLI Lab Setup\n");

  const backSelection = "__back__";
  const noneSelection = "__none__";
  const allSelection = "__all__";
  const searchRequested = partialConfig.search === true;
  const promptedValues = {};

  function buildSteps() {
    const topology = resolvePromptValue(promptedValues, partialConfig, "topology", DEFAULT_UP_OPTIONS.topology);
    const mongodbVersion = resolvePromptValue(
      promptedValues,
      partialConfig,
      "mongodbVersion",
      DEFAULT_UP_OPTIONS.mongodbVersion
    );
    const steps = [];

    if (!hasValue(partialConfig.topology)) {
      steps.push("topology");
    }

    if (!hasValue(partialConfig.shardCount) && topology === "sharded") {
      steps.push("shardCount");
    }

    if (!hasValue(partialConfig.replicaSetMembers) && topology !== "standalone") {
      steps.push("replicaSetMembers");
    }

    if (!hasValue(partialConfig.mongodbVersion)) {
      steps.push("mongodbVersion");
    }

    if (!hasValue(partialConfig.mongosPort)) {
      steps.push("mongosPort");
    }

    if (!hasValue(partialConfig.search) && supportsSearchTopology(topology) && supportsSearch(mongodbVersion)) {
      steps.push("search");
    }

    if (!hasValue(partialConfig.sampleDatabases)) {
      steps.push("sampleDatabases");
    }

    return steps;
  }

  let stepIndex = 0;

  while (true) {
    const steps = buildSteps();
    if (!steps.length) {
      return {};
    }

    if (stepIndex < 0) {
      return { cancelled: true };
    }

    if (stepIndex >= steps.length) {
      break;
    }

    const step = steps[stepIndex];

    if (step === "topology") {
      const choices = [
        { name: "Standalone", value: "standalone" },
        { name: "Replica set", value: "replica-set" },
        ...(!searchRequested ? [{ name: "Sharded cluster", value: "sharded" }] : []),
        { name: "Back", value: backSelection }
      ];
      const { topology } = await inquirer.prompt([
        {
          type: "list",
          name: "topology",
          message: "Which cluster topology do you want?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "topology", DEFAULT_UP_OPTIONS.topology)
          )
        }
      ]);

      if (topology === backSelection) {
        stepIndex -= 1;
        continue;
      }

      promptedValues.topology = topology;
      stepIndex += 1;
      continue;
    }

    if (step === "shardCount") {
      const choices = [
        { name: "1 shard", value: 1 },
        { name: "2 shards", value: 2 },
        { name: "3 shards", value: 3 },
        { name: "4 shards", value: 4 },
        { name: "Custom", value: "custom" },
        { name: "Back", value: backSelection }
      ];
      const { shardCount } = await inquirer.prompt([
        {
          type: "list",
          name: "shardCount",
          message: "How many shards do you want?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "shardCount", DEFAULT_UP_OPTIONS.shards)
          )
        }
      ]);

      if (shardCount === backSelection) {
        stepIndex -= 1;
        continue;
      }

      if (shardCount === "custom") {
        const { customShardCount } = await inquirer.prompt([
          {
            type: "input",
            name: "customShardCount",
            message: "Enter the number of shards:",
            validate: (value) =>
              value.trim().toLowerCase() === "back" || validatePositiveInteger("Shard count")(Number(value))
          }
        ]);

        if (customShardCount.trim().toLowerCase() === "back") {
          continue;
        }

        promptedValues.shardCount = Number(customShardCount);
      } else {
        promptedValues.shardCount = shardCount;
      }

      stepIndex += 1;
      continue;
    }

    if (step === "replicaSetMembers") {
      const topology = resolvePromptValue(promptedValues, partialConfig, "topology", DEFAULT_UP_OPTIONS.topology);
      const choices = [
        { name: "1 member", value: 1 },
        { name: "3 members", value: 3 },
        { name: "5 members", value: 5 },
        { name: "Custom", value: "custom" },
        { name: "Back", value: backSelection }
      ];
      const { replicaSetMembers } = await inquirer.prompt([
        {
          type: "list",
          name: "replicaSetMembers",
          message: topology === "sharded"
            ? "How many replica set members per shard?"
            : "How many replica set members do you want?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "replicaSetMembers", DEFAULT_UP_OPTIONS.replicas)
          )
        }
      ]);

      if (replicaSetMembers === backSelection) {
        stepIndex -= 1;
        continue;
      }

      if (replicaSetMembers === "custom") {
        const { customReplicaSetMembers } = await inquirer.prompt([
          {
            type: "input",
            name: "customReplicaSetMembers",
            message: "Enter replica set members:",
            validate: (value) =>
              value.trim().toLowerCase() === "back" || validatePositiveInteger("Replica set members")(Number(value))
          }
        ]);

        if (customReplicaSetMembers.trim().toLowerCase() === "back") {
          continue;
        }

        promptedValues.replicaSetMembers = Number(customReplicaSetMembers);
      } else {
        promptedValues.replicaSetMembers = replicaSetMembers;
      }

      stepIndex += 1;
      continue;
    }

    if (step === "mongodbVersion") {
      const choices = searchRequested
        ? [
            { name: "8.2", value: "8.2" },
            { name: "Back", value: backSelection }
          ]
        : [
            { name: "8.2", value: "8.2" },
            { name: "8.0", value: "8.0" },
            { name: "7.0", value: "7.0" },
            { name: "6.0", value: "6.0" },
            { name: "5.0", value: "5.0" },
            { name: "Custom", value: "Custom" },
            { name: "Back", value: backSelection }
          ];
      const { mongodbVersion } = await inquirer.prompt([
        {
          type: "list",
          name: "mongodbVersion",
          message: "Which MongoDB version should be used?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "mongodbVersion", DEFAULT_UP_OPTIONS.mongodbVersion)
          )
        }
      ]);

      if (mongodbVersion === backSelection) {
        stepIndex -= 1;
        continue;
      }

      if (mongodbVersion === "Custom") {
        const { customMongodbVersion } = await inquirer.prompt([
          {
            type: "input",
            name: "customMongodbVersion",
            message: "Enter the MongoDB Docker tag/version:",
            default: DEFAULT_UP_OPTIONS.mongodbVersion,
            validate: (value) =>
              value.trim().toLowerCase() === "back" || (value.trim() ? true : "Version cannot be empty.")
          }
        ]);

        if (customMongodbVersion.trim().toLowerCase() === "back") {
          continue;
        }

        promptedValues.mongodbVersion = customMongodbVersion.trim();
      } else {
        promptedValues.mongodbVersion = mongodbVersion;
      }

      stepIndex += 1;
      continue;
    }

    if (step === "mongosPort") {
      const topology = resolvePromptValue(promptedValues, partialConfig, "topology", DEFAULT_UP_OPTIONS.topology);
      const choices = [
        { name: "27017", value: 27017 },
        { name: "28000", value: 28000 },
        { name: "30000", value: 30000 },
        { name: "Custom", value: "custom" },
        { name: "Back", value: backSelection }
      ];
      const { mongosPort } = await inquirer.prompt([
        {
          type: "list",
          name: "mongosPort",
          message: topology === "sharded"
            ? "Which port should mongos expose?"
            : topology === "replica-set"
              ? "Which base port should the replica set use?"
              : "Which port should MongoDB expose?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "mongosPort", DEFAULT_UP_OPTIONS.port)
          )
        }
      ]);

      if (mongosPort === backSelection) {
        stepIndex -= 1;
        continue;
      }

      if (mongosPort === "custom") {
        const { customMongosPort } = await inquirer.prompt([
          {
            type: "input",
            name: "customMongosPort",
            message: topology === "sharded"
              ? "Enter the mongos port:"
              : topology === "replica-set"
                ? "Enter the base port for the replica set:"
                : "Enter the MongoDB port:",
            validate: (value) =>
              value.trim().toLowerCase() === "back" || validatePort(Number(value))
          }
        ]);

        if (customMongosPort.trim().toLowerCase() === "back") {
          continue;
        }

        promptedValues.mongosPort = Number(customMongosPort);
      } else {
        promptedValues.mongosPort = mongosPort;
      }

      stepIndex += 1;
      continue;
    }

    if (step === "search") {
      const choices = [
        { name: "Enable", value: true },
        { name: "Disable", value: false },
        { name: "Back", value: backSelection }
      ];
      const { search } = await inquirer.prompt([
        {
          type: "list",
          name: "search",
          message: "Enable MongoDB Search support in this cluster?",
          choices,
          default: getChoiceDefaultIndex(
            choices,
            resolvePromptValue(promptedValues, partialConfig, "search", DEFAULT_UP_OPTIONS.search)
          )
        }
      ]);

      if (search === backSelection) {
        stepIndex -= 1;
        continue;
      }

      promptedValues.search = search;
      stepIndex += 1;
      continue;
    }

    if (step === "sampleDatabases") {
      const { sampleDatabases } = await inquirer.prompt([
        {
          type: "checkbox",
          name: "sampleDatabases",
          message: "Choose sample databases to prepare for this lab",
          choices: [
            { name: "None", value: noneSelection },
            { name: "All available sample databases", value: allSelection },
            ...AVAILABLE_SAMPLE_DATABASES.map((databaseName) => ({
              name: databaseName,
              value: databaseName
            })),
            { name: "Back", value: backSelection }
          ],
          validate(value) {
            if (!value.length) {
              return "Choose at least one option to continue.";
            }

            if (value.includes(backSelection) && value.length > 1) {
              return "Select only 'Back' to return to the previous step.";
            }

            return true;
          }
        }
      ]);

      if (sampleDatabases.includes(backSelection)) {
        stepIndex -= 1;
        continue;
      }

      promptedValues.sampleDatabases = sampleDatabases.includes(noneSelection)
        ? []
        : sampleDatabases.includes(allSelection)
          ? [...AVAILABLE_SAMPLE_DATABASES]
          : sampleDatabases;
      stepIndex += 1;
    }
  }

  return promptedValues;
}

export async function resolveUpConfig(options = {}) {
  if (options.quickstart) {
    return buildQuickstartConfig(options);
  }

  const partialConfig = {
    topology: options.topology,
    shardCount: options.shards,
    replicaSetMembers: options.replicas,
    mongodbVersion: options.mongodbVersion,
    mongosPort: options.port,
    storagePath: options.storagePath ?? DEFAULT_UP_OPTIONS.storagePath,
    search: options.search,
    sampleDatabases: hasValue(options.sampleDatabases) ? normalizeSampleDatabases(options.sampleDatabases) : undefined,
    searchMongodPort: options.searchMongodPort,
    searchPort: options.searchPort,
    metricsPort: options.metricsPort
  };

  const promptedValues = await promptForMissingValues(partialConfig);
  if (promptedValues.cancelled) {
    return null;
  }

  const resolvedConfig = normalizeClusterConfig({
    topology: promptedValues.topology ?? partialConfig.topology,
    shardCount: promptedValues.shardCount ?? partialConfig.shardCount,
    replicaSetMembers: promptedValues.replicaSetMembers ?? partialConfig.replicaSetMembers,
    mongodbVersion: promptedValues.mongodbVersion ?? partialConfig.mongodbVersion,
    mongosPort: promptedValues.mongosPort ?? partialConfig.mongosPort,
    storagePath: partialConfig.storagePath,
    search: promptedValues.search ?? partialConfig.search,
    sampleDatabases: promptedValues.sampleDatabases ?? partialConfig.sampleDatabases,
    searchMongodPort: partialConfig.searchMongodPort,
    searchPort: partialConfig.searchPort,
    metricsPort: partialConfig.metricsPort
  });

  if (resolvedConfig.features.search && !supportsSearch(resolvedConfig.mongodbVersion)) {
    throw new Error("MongoDB Search requires MongoDB 8.2 or newer.");
  }

  if (resolvedConfig.features.search && !supportsSearchTopology(resolvedConfig.topology)) {
    throw new Error("MongoDB Search support is currently available only for standalone and replica-set topologies.");
  }

  return resolvedConfig;
}

export function getAvailableSampleDatabases() {
  return [...AVAILABLE_SAMPLE_DATABASES];
}
