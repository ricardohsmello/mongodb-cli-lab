import { existsSync } from "node:fs";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import inquirer from "inquirer";

const GLOBAL_STATE_DIR = path.join(os.homedir(), ".mongodb-cli-lab");
const SEARCH_STATE_FILE_NAME = ".mongodb-cli-lab-search-state.json";
const DEFAULT_SEARCH_STORAGE_PATH = "./mongodb-search-lab";
const DEFAULT_SEARCH_PASSWORD = "mongotPassword";
const DEFAULT_MONGOD_PORT = 27017;
const DEFAULT_MONGOT_PORT = 27028;
const DEFAULT_METRICS_PORT = 9946;
const DEFAULT_HEALTH_PORT = 8080;
const DEFAULT_PROJECT_NAME = "mongodb-cli-lab-search";
const DEFAULT_NETWORK_NAME = "search-community";
const DEFAULT_MONGOD_IMAGE = "mongodb/mongodb-community-server:latest";
const DEFAULT_MONGOT_IMAGE = "mongodb/mongodb-community-search:latest";
const SAMPLE_DATA_URL = "https://atlas-education.s3.amazonaws.com/sampledata.archive";
const AVAILABLE_SAMPLE_DATABASES = Object.freeze([
  {
    name: "sample_airbnb",
    collectionName: "listingsAndReviews",
    description: "Sample Airbnb listings and reviews data."
  },
  {
    name: "sample_mflix",
    collectionName: "movies",
    description: "Sample movies dataset used for Search demos."
  }
]);

let dockerComposeCommand = null;

function runCommand(command, args, options = {}) {
  return execFileSync(command, args, {
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    encoding: options.capture ? "utf8" : undefined
  });
}

function tryRunCommand(command, args, options = {}) {
  try {
    return runCommand(command, args, options);
  } catch {
    return null;
  }
}

function getErrorText(error) {
  return [
    error?.message,
    typeof error?.stderr === "string" ? error.stderr : "",
    typeof error?.stdout === "string" ? error.stdout : ""
  ]
    .filter(Boolean)
    .join("\n");
}

function detectDockerComposeCommand() {
  const candidates = [
    ["docker", ["compose"]],
    ["docker-compose", []]
  ];

  for (const [command, baseArgs] of candidates) {
    try {
      runCommand(command, [...baseArgs, "version"], { capture: true });
      return { command, baseArgs };
    } catch {
      // Try the next candidate.
    }
  }

  return null;
}

function ensureDockerAvailable() {
  try {
    runCommand("docker", ["--version"]);
  } catch {
    throw new Error("Docker is required and was not found in PATH.");
  }

  dockerComposeCommand = detectDockerComposeCommand();
  if (!dockerComposeCommand) {
    throw new Error(
      "Docker Compose is required and was not found. Install a Docker version that provides 'docker compose' or 'docker-compose'."
    );
  }
}

function sanitizeProjectName(value) {
  return value.replace(/[^a-zA-Z0-9_-]/g, "-");
}

function sleep(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function composeArgs(state, args = []) {
  if (!dockerComposeCommand) {
    throw new Error("Docker Compose command is not initialized.");
  }

  return [
    ...dockerComposeCommand.baseArgs,
    "-f",
    state.composeFile,
    "-p",
    state.projectName,
    ...args
  ];
}

function runCompose(state, args = [], options = {}) {
  if (!dockerComposeCommand) {
    throw new Error("Docker Compose command is not initialized.");
  }

  return runCommand(dockerComposeCommand.command, composeArgs(state, args), options);
}

function getStateFilePath(storagePath) {
  return path.join(storagePath, SEARCH_STATE_FILE_NAME);
}

function getGlobalStateFilePath() {
  return path.join(GLOBAL_STATE_DIR, SEARCH_STATE_FILE_NAME);
}

async function tryReadStateFile(stateFilePath) {
  if (!existsSync(stateFilePath)) {
    return null;
  }

  const raw = await fs.readFile(stateFilePath, "utf8");
  return JSON.parse(raw);
}

async function saveState(state) {
  await fs.mkdir(GLOBAL_STATE_DIR, { recursive: true });
  await fs.writeFile(getGlobalStateFilePath(), JSON.stringify(state, null, 2), "utf8");
  await fs.writeFile(getStateFilePath(state.config.storagePath), JSON.stringify(state, null, 2), "utf8");
}

async function deleteState(storagePath) {
  const globalStateFilePath = getGlobalStateFilePath();
  if (existsSync(globalStateFilePath)) {
    await fs.unlink(globalStateFilePath);
  }

  const stateFilePath = getStateFilePath(storagePath);
  if (existsSync(stateFilePath)) {
    await fs.unlink(stateFilePath);
  }
}

function buildSearchConfig(options = {}) {
  return {
    kind: "search",
    projectName: sanitizeProjectName(DEFAULT_PROJECT_NAME),
    storagePath: path.resolve(process.cwd(), options.storagePath ?? DEFAULT_SEARCH_STORAGE_PATH),
    networkName: options.networkName ?? DEFAULT_NETWORK_NAME,
    mongodPort: options.port ?? DEFAULT_MONGOD_PORT,
    mongotPort: options.searchPort ?? DEFAULT_MONGOT_PORT,
    metricsPort: options.metricsPort ?? DEFAULT_METRICS_PORT,
    healthPort: DEFAULT_HEALTH_PORT,
    mongodImage: options.mongodImage ?? DEFAULT_MONGOD_IMAGE,
    mongotImage: options.mongotImage ?? DEFAULT_MONGOT_IMAGE,
    sampleDataUrl: SAMPLE_DATA_URL,
    sampleDataFileName: "sampledata.archive",
    password: options.password ?? DEFAULT_SEARCH_PASSWORD,
    databaseName: "sample_mflix",
    collectionName: "movies",
    searchIndexName: "default"
  };
}

function configsMatch(left, right) {
  return (
    left.storagePath === right.storagePath &&
    left.networkName === right.networkName &&
    left.mongodPort === right.mongodPort &&
    left.mongotPort === right.mongotPort &&
    left.metricsPort === right.metricsPort &&
    left.mongodImage === right.mongodImage &&
    left.mongotImage === right.mongotImage
  );
}

async function confirmAction(message, defaultValue = false) {
  const { confirmed } = await inquirer.prompt([
    {
      type: "list",
      name: "confirmed",
      message,
      choices: [
        { name: defaultValue ? "Continue" : "Confirm", value: true },
        { name: "Back", value: false }
      ],
      default: true
    }
  ]);

  return confirmed;
}

function printStep(stepNumber, totalSteps, title, description) {
  console.log(`\n=== Step ${stepNumber}/${totalSteps}: ${title} ===`);
  if (description) {
    console.log(description);
  }
}

function createSearchComposeFile(config) {
  return `services:
  mongod:
    image: ${config.mongodImage}
    command:
      - "mongod"
      - "--config"
      - "/etc/mongod.conf"
      - "--replSet"
      - "rs0"
    ports:
      - "${config.mongodPort}:27017"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - "mongod_data:/data/db"
      - "${path.join(config.storagePath, "mongod.conf")}:/etc/mongod.conf:ro"
      - "${path.join(config.storagePath, config.sampleDataFileName)}:/sampledata.archive"
    networks:
      - ${config.networkName}
  mongot:
    image: ${config.mongotImage}
    networks:
      - ${config.networkName}
    volumes:
      - "mongot_data:/data/mongot"
      - "${path.join(config.storagePath, "mongot.conf")}:/mongot-community/config.default.yml:ro"
      - "${path.join(config.storagePath, "pwfile")}:/mongot-community/pwfile:ro"
    depends_on:
      - mongod
    ports:
      - "${config.mongotPort}:27028"
      - "${config.metricsPort}:9946"
volumes:
  mongod_data:
  mongot_data:
networks:
  ${config.networkName}:
    name: ${config.networkName}
    external: true
`;
}

function createMongodConfig(config) {
  return `storage:
  dbPath: /data/db
net:
  port: 27017
  bindIp: 0.0.0.0
setParameter:
  searchIndexManagementHostAndPort: mongot.${config.networkName}:27028
  mongotHost: mongot.${config.networkName}:27028
  skipAuthenticationToSearchIndexManagementServer: false
  useGrpcForSearch: true
replication:
  replSetName: rs0
`;
}

function createMongotConfig(config) {
  return `syncSource:
  replicaSet:
    hostAndPort: "mongod.${config.networkName}:27017"
    username: mongotUser
    passwordFile: /mongot-community/pwfile
    authSource: admin
    tls: false
    readPreference: primaryPreferred
storage:
  dataPath: "data/mongot"
server:
  grpc:
    address: "mongot.${config.networkName}:27028"
    tls:
      mode: "disabled"
metrics:
  enabled: true
  address: "mongot.${config.networkName}:9946"
healthCheck:
  address: "mongot.${config.networkName}:${config.healthPort}"
logging:
  verbosity: INFO
`;
}

function createInitMongoScript(config) {
  return `#!/bin/bash
set -e
echo "MongoDB Search Lab init hook detected. Runtime initialization is handled by mongodb-cli-lab after mongod starts."
`;
}

async function writeSearchFiles(config) {
  await fs.mkdir(config.storagePath, { recursive: true });

  const files = {
    composeFile: path.join(config.storagePath, "docker-compose.yml"),
    mongodConfigFile: path.join(config.storagePath, "mongod.conf"),
    mongotConfigFile: path.join(config.storagePath, "mongot.conf"),
    initScriptFile: path.join(config.storagePath, "init-mongo.sh"),
    passwordFile: path.join(config.storagePath, "pwfile"),
    sampleDataArchiveFile: path.join(config.storagePath, config.sampleDataFileName),
    configFile: path.join(config.storagePath, "search-config.json")
  };

  if (existsSync(files.passwordFile)) {
    await fs.chmod(files.passwordFile, 0o600);
  }

  await Promise.all([
    fs.writeFile(files.composeFile, createSearchComposeFile(config), "utf8"),
    fs.writeFile(files.mongodConfigFile, createMongodConfig(config), "utf8"),
    fs.writeFile(files.mongotConfigFile, createMongotConfig(config), "utf8"),
    fs.writeFile(files.initScriptFile, createInitMongoScript(config), { encoding: "utf8", mode: 0o755 }),
    fs.writeFile(files.passwordFile, config.password, { encoding: "utf8", mode: 0o400 }),
    fs.writeFile(files.configFile, JSON.stringify(config, null, 2), "utf8")
  ]);

  return files;
}

function isPortExplicitlyConfigured(options, key) {
  return options[key] !== undefined && options[key] !== null;
}

function canBindToPort(port) {
  return new Promise((resolve) => {
    const server = net.createServer();

    server.once("error", () => {
      resolve(false);
    });

    server.once("listening", () => {
      server.close(() => resolve(true));
    });

    server.listen(port, "0.0.0.0");
  });
}

async function findAvailablePort(startPort) {
  for (let port = startPort; port < startPort + 200; port += 1) {
    if (await canBindToPort(port)) {
      return port;
    }
  }

  throw new Error(`Could not find an available port near ${startPort}.`);
}

async function resolvePortConflicts(state, options = {}) {
  const mappings = [
    { key: "mongodPort", optionKey: "port", label: "mongod" },
    { key: "mongotPort", optionKey: "searchPort", label: "mongot" },
    { key: "metricsPort", optionKey: "metricsPort", label: "mongot metrics" }
  ];

  let hasChanges = false;

  for (const mapping of mappings) {
    const currentPort = state.config[mapping.key];
    const available = await canBindToPort(currentPort);
    if (available) {
      continue;
    }

    if (isPortExplicitlyConfigured(options, mapping.optionKey)) {
      throw new Error(
        `Port ${currentPort} for ${mapping.label} is already in use. Choose a different port and rerun the command.`
      );
    }

    const nextPort = await findAvailablePort(currentPort + 1);
    console.log(`Port ${currentPort} for ${mapping.label} is in use. Switching to ${nextPort}.`);
    state.config[mapping.key] = nextPort;
    hasChanges = true;
  }

  if (!hasChanges) {
    return state;
  }

  const files = await writeSearchFiles(state.config);
  Object.assign(state, files);
  await saveState(state);
  return state;
}

async function loadStateFromStoragePath(storagePath) {
  const composeFile = path.join(storagePath, "docker-compose.yml");
  const configFile = path.join(storagePath, "search-config.json");

  if (!existsSync(composeFile) || !existsSync(configFile)) {
    return null;
  }

  const configRaw = await fs.readFile(configFile, "utf8");
  const config = JSON.parse(configRaw);

  return {
    projectName: config.projectName,
    config,
    composeFile,
    configFile,
    mongodConfigFile: path.join(storagePath, "mongod.conf"),
    mongotConfigFile: path.join(storagePath, "mongot.conf"),
    initScriptFile: path.join(storagePath, "init-mongo.sh"),
    passwordFile: path.join(storagePath, "pwfile"),
    sampleDataArchiveFile: path.join(storagePath, config.sampleDataFileName)
  };
}

function listProjectContainerIds(projectName, serviceName = null) {
  const args = [
    "ps",
    "-a",
    "--filter",
    `label=com.docker.compose.project=${projectName}`
  ];

  if (serviceName) {
    args.push("--filter", `label=com.docker.compose.service=${serviceName}`);
  }

  args.push("--format", "{{.ID}}");

  const output = tryRunCommand("docker", args, { capture: true })?.trim() ?? "";
  if (!output) {
    return [];
  }

  return output
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

async function discoverStateFromDocker() {
  const projectName = sanitizeProjectName(DEFAULT_PROJECT_NAME);
  const containerIds = [
    ...listProjectContainerIds(projectName, "mongod"),
    ...listProjectContainerIds(projectName)
  ];

  if (!containerIds.length) {
    return null;
  }

  const inspectOutput = tryRunCommand("docker", ["inspect", containerIds[0]], { capture: true });
  if (!inspectOutput) {
    return null;
  }

  let details;
  try {
    details = JSON.parse(inspectOutput);
  } catch {
    return null;
  }

  const mounts = Array.isArray(details?.[0]?.Mounts) ? details[0].Mounts : [];
  const composeMount = mounts.find((mount) => typeof mount?.Source === "string" && mount.Source.endsWith(`${path.sep}mongod.conf`));
  if (!composeMount) {
    return null;
  }

  return loadStateFromStoragePath(path.dirname(composeMount.Source));
}

async function loadState() {
  const activeState = await discoverStateFromDocker();
  if (activeState) {
    await saveState(activeState);
    return activeState;
  }

  const globalState = await tryReadStateFile(getGlobalStateFilePath());
  if (globalState) {
    return globalState;
  }

  const localStatePath = getStateFilePath(path.resolve(process.cwd(), DEFAULT_SEARCH_STORAGE_PATH));
  const localState = await tryReadStateFile(localStatePath);
  if (!localState) {
    return null;
  }

  await saveState(localState);
  return localState;
}

function getComposeContainers(state) {
  try {
    const output = runCompose(state, ["ps", "--format", "json"], { capture: true });
    const trimmedOutput = output.trim();

    if (!trimmedOutput) {
      return [];
    }

    if (trimmedOutput.startsWith("[")) {
      const parsed = JSON.parse(trimmedOutput);
      return Array.isArray(parsed) ? parsed : [];
    }

    return trimmedOutput
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

function formatPorts(publishers = []) {
  const ports = publishers
    .map((publisher) => {
      if (!publisher.PublishedPort) {
        return null;
      }

      return `${publisher.PublishedPort}->${publisher.TargetPort}/${publisher.Protocol}`;
    })
    .filter(Boolean);

  return ports.length ? ports.join(", ") : "-";
}

function printStatus(state, containers) {
  const runningCount = containers.filter((container) => container.State === "running").length;
  const hostConnectionString = `mongodb://localhost:${state.config.mongodPort}/?directConnection=true`;

  console.log("\nMongoDB Search Lab\n");
  console.log(`Project: ${state.projectName}`);
  console.log(`Storage: ${state.config.storagePath}`);
  console.log(`Network: ${state.config.networkName}`);
  console.log(`mongod: ${hostConnectionString}`);
  console.log(`mongot: localhost:${state.config.mongotPort}`);
  console.log(`Nodes running: ${runningCount}/2\n`);

  if (!containers.length) {
    console.log("No containers found for this search lab.\n");
    return;
  }

  console.table(
    containers.map((container) => ({
      service: container.Service,
      state: container.State,
      ports: formatPorts(container.Publishers)
    }))
  );
}

function runMongoScriptCapture(state, serviceName, script) {
  return runCompose(
    state,
    ["exec", "-T", serviceName, "mongosh", "--quiet", "--eval", script],
    { capture: true }
  );
}

function runMongoScript(state, serviceName, script) {
  runCompose(state, ["exec", "-T", serviceName, "mongosh", "--quiet", "--eval", script]);
}

function runContainerCommand(state, serviceName, args = [], options = {}) {
  return runCompose(state, ["exec", "-T", serviceName, ...args], options);
}

function runMongoJson(state, serviceName, script) {
  const marker = "__MONGODB_CLI_LAB_SEARCH_JSON__";
  const output = runMongoScriptCapture(
    state,
    serviceName,
    `${script}\nprint("${marker}" + JSON.stringify(result));`
  );

  const line = output
    .split("\n")
    .map((entry) => entry.trim())
    .find((entry) => entry.startsWith(marker));

  if (!line) {
    throw new Error("Could not parse MongoDB Search command output.");
  }

  return JSON.parse(line.slice(marker.length));
}

function getCollectionCount(state, databaseName, collectionName) {
  return runMongoJson(
    state,
    "mongod",
    `
const result = {
  count: db.getSiblingDB(${JSON.stringify(databaseName)}).getCollection(${JSON.stringify(collectionName)}).countDocuments({})
};
`.trim()
  ).count;
}

function getSampleDatabaseDefinitions(databaseNames) {
  const definitions = AVAILABLE_SAMPLE_DATABASES.filter((database) => databaseNames.includes(database.name));

  if (definitions.length !== databaseNames.length) {
    const available = AVAILABLE_SAMPLE_DATABASES.map((database) => database.name).join(", ");
    throw new Error(`Unknown sample database selection. Available databases: ${available}.`);
  }

  return definitions;
}

function getRestoredSampleDatabaseCounts(state, databaseNames) {
  const definitions = getSampleDatabaseDefinitions(databaseNames);

  return definitions.map((database) => ({
    ...database,
    count: getCollectionCount(state, database.name, database.collectionName)
  }));
}

function ensureSampleDatabasesRestored(state, databaseNames) {
  const definitions = getSampleDatabaseDefinitions(databaseNames);
  console.log(`Checking sample data for ${definitions.map((database) => database.name).join(", ")}...`);

  try {
    const counts = getRestoredSampleDatabaseCounts(state, databaseNames);
    if (counts.every((database) => database.count > 0)) {
      console.log("Selected sample databases are already available.");
      return counts;
    }
  } catch {
    // Continue to restore.
  }

  console.log("Restoring selected sample databases into mongod...");
  runContainerCommand(state, "mongod", [
    "mongorestore",
    "--archive=/sampledata.archive",
    ...definitions.map((database) => `--nsInclude=${database.name}.*`)
  ]);

  for (let attempt = 0; attempt < 180; attempt += 1) {
    try {
      const counts = getRestoredSampleDatabaseCounts(state, databaseNames);
      if (counts.every((database) => database.count > 0)) {
        console.log("Selected sample databases restored successfully.");
        return counts;
      }
    } catch {
      // keep waiting
    }

    if (attempt > 0 && attempt % 15 === 0) {
      console.log(`Still waiting for sample data (${attempt}s elapsed)...`);
    }

    sleep(1000);
  }

  throw new Error("Timed out waiting for the selected sample databases to be restored.");
}

function waitForMongoService(state, serviceName) {
  console.log(`Waiting for MongoDB service '${serviceName}'...`);

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      runCompose(state, [
        "exec",
        "-T",
        serviceName,
        "mongosh",
        "--quiet",
        "--eval",
        "db.adminCommand({ ping: 1 })"
      ]);
      console.log(`MongoDB service '${serviceName}' is ready.`);
      return;
    } catch {
      if (attempt > 0 && attempt % 10 === 0) {
        console.log(`Still waiting for '${serviceName}' (${attempt}s elapsed)...`);
      }
      sleep(1000);
    }
  }

  throw new Error(`Timed out waiting for MongoDB service '${serviceName}'.`);
}

function ensureReplicaPrimary(state) {
  console.log("Ensuring replica set is initialized...");
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      runMongoScript(
        state,
        "mongod",
        `
const replicaConfig = {
  _id: "rs0",
  members: [
    { _id: 0, host: ${JSON.stringify(`mongod.${state.config.networkName}:27017`)} }
  ]
};

try {
  const status = db.adminCommand({ replSetGetStatus: 1 });
  if (status.ok === 1) {
    print("Replica set already initialized");
  }
} catch (error) {
  if (error.code === 94) {
    print("Initializing replica set...");
    printjson(rs.initiate(replicaConfig));
  } else {
    throw error;
  }
}
`.trim()
      );
      break;
    } catch (error) {
      const message = getErrorText(error);
      const retriable =
        message.includes("ECONNREFUSED") ||
        message.includes("connect ECONNREFUSED") ||
        message.includes("Connection refused") ||
        message.includes("connection refused") ||
        message.includes("No host described") ||
        message.includes("HostUnreachable") ||
        message.includes("not running with --replSet");

      if (!retriable) {
        throw error;
      }

      if (attempt > 0 && attempt % 10 === 0) {
        console.log(`Still waiting to initialize replica set (${attempt}s elapsed)...`);
      }
      sleep(1000);
    }
  }

  console.log("Waiting for mongod to become primary...");
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const result = runMongoJson(
        state,
        "mongod",
        `
const hello = db.hello();
const result = {
  isWritablePrimary: hello.isWritablePrimary === true || hello.ismaster === true
};
`.trim()
      );

      if (result.isWritablePrimary) {
        console.log("mongod is now primary.");
        return;
      }
    } catch {
      // keep waiting
    }

    if (attempt > 0 && attempt % 10 === 0) {
      console.log(`Still waiting for primary (${attempt}s elapsed)...`);
    }
    sleep(1000);
  }

  throw new Error("Timed out waiting for mongod to become primary.");
}

function ensureSearchCoordinatorUser(state) {
  runMongoScript(
    state,
    "mongod",
    `
const adminDb = db.getSiblingDB("admin");
const username = "mongotUser";
const password = ${JSON.stringify(state.config.password)};
const existing = adminDb.getUser(username);

if (!existing) {
  adminDb.createUser({
    user: username,
    pwd: password,
    roles: [{ role: "searchCoordinator", db: "admin" }]
  });
  print("Created mongotUser.");
} else {
  adminDb.updateUser(username, {
    pwd: password,
    roles: [{ role: "searchCoordinator", db: "admin" }]
  });
  print("Updated mongotUser.");
}
`.trim()
  );
}

function isServiceRunning(state, serviceName) {
  return getComposeContainers(state).some(
    (container) => container.Service === serviceName && container.State === "running"
  );
}

function ensureSearchIndex(state) {
  return runMongoJson(
    state,
    "mongod",
    `
const database = db.getSiblingDB(${JSON.stringify(state.config.databaseName)});
const collection = database.getCollection(${JSON.stringify(state.config.collectionName)});
const existing = collection
  .getSearchIndexes()
  .find((index) => index.name === ${JSON.stringify(state.config.searchIndexName)});

let action = "reuse";
if (!existing) {
  collection.createSearchIndex(
    ${JSON.stringify(state.config.searchIndexName)},
    { mappings: { dynamic: true } }
  );
  action = "create";
}

const result = { action };
`.trim()
  );
}

function waitForSearchIndexManagement(state) {
  console.log("Waiting for Search Index Management to become available...");

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      return ensureSearchIndex(state);
    } catch (error) {
      const message = getErrorText(error);
      const retriable =
        message.includes("Error connecting to Search Index Management service") ||
        message.includes("Connection refused") ||
        message.includes("timed out") ||
        message.includes("HostUnreachable");

      if (!retriable) {
        throw error;
      }
    }

    if (attempt > 0 && attempt % 10 === 0) {
      console.log(`Still waiting for Search Index Management (${attempt}s elapsed)...`);
    }

    sleep(1000);
  }

  throw new Error("Timed out waiting for Search Index Management to become available.");
}

function waitForSearchQuery(state) {
  console.log("Waiting for the search index to become queryable...");

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const result = runMongoJson(
        state,
        "mongod",
        `
const documents = db.getSiblingDB(${JSON.stringify(state.config.databaseName)})
  .getCollection(${JSON.stringify(state.config.collectionName)})
  .aggregate([
    {
      $search: {
        text: {
          query: "baseball",
          path: "plot"
        }
      }
    },
    { $limit: 5 },
    {
      $project: {
        _id: 0,
        title: 1,
        plot: 1
      }
    }
  ])
  .toArray();

const result = { documents };
`.trim()
      );

      if (result.documents.length) {
        return result.documents;
      }
    } catch {
      // keep waiting
    }

    if (attempt > 0 && attempt % 10 === 0) {
      console.log(`Still waiting for search results (${attempt}s elapsed)...`);
    }

    sleep(1000);
  }

  throw new Error("Timed out waiting for MongoDB Search to return results.");
}

function printQuickstartPlan(config) {
  console.log("\nSearch quickstart\n");
  console.log("This command will:");
  console.log(`- create a Search lab in ${config.storagePath}`);
  console.log(`- use ${config.mongodImage}`);
  console.log(`- use ${config.mongotImage}`);
  console.log(`- expose mongod on localhost:${config.mongodPort}`);
  console.log(`- expose mongot gRPC on localhost:${config.mongotPort}`);
  console.log("- download the sample archive");
  console.log("- restore sample_mflix");
  console.log('- create the "default" Search index on sample_mflix.movies');
  console.log('- run a $search query for "baseball"\n');
}

async function ensureSearchNetwork(state) {
  const result = tryRunCommand("docker", ["network", "create", state.config.networkName], { capture: true });
  if (result) {
    console.log(`Created Docker network '${state.config.networkName}'.`);
    return;
  }

  const inspectResult = tryRunCommand("docker", ["network", "inspect", state.config.networkName], { capture: true });
  if (!inspectResult) {
    throw new Error(`Could not create or inspect Docker network '${state.config.networkName}'.`);
  }

  console.log(`Using existing Docker network '${state.config.networkName}'.`);
}

async function ensureSearchAssets(state) {
  printStep(
    1,
    3,
    "Pull Docker images",
    "Downloading the MongoDB Community Server and Community Search images required for the Search lab."
  );
  runCommand("docker", ["pull", state.config.mongodImage]);
  runCommand("docker", ["pull", state.config.mongotImage]);

  printStep(
    2,
    3,
    "Download sample data",
    "Downloading the sample archive used by the Search quickstart flow."
  );
  if (!existsSync(state.sampleDataArchiveFile)) {
    runCommand("curl", ["-L", state.config.sampleDataUrl, "-o", state.sampleDataArchiveFile]);
  } else {
    console.log(`Sample archive already exists at ${state.sampleDataArchiveFile}.`);
  }

  printStep(
    3,
    3,
    "Prepare configuration files",
    "Writing docker-compose and configuration files for mongod and mongot."
  );
  await writeSearchFiles(state.config);
}

function ensureSampleArchiveExists(state) {
  if (existsSync(state.sampleDataArchiveFile)) {
    return;
  }

  console.log("Downloading the sample archive used by the Search lab.");
  runCommand("curl", ["-L", state.config.sampleDataUrl, "-o", state.sampleDataArchiveFile]);
}

function removeContainerIfExists(containerName) {
  const containerId = tryRunCommand("docker", ["ps", "-a", "--filter", `name=^/${containerName}$`, "--format", "{{.ID}}"], {
    capture: true
  })?.trim();

  if (!containerId) {
    return;
  }

  runCommand("docker", ["rm", "-f", containerName]);
}

function cleanupLegacySearchContainers() {
  // Older versions of the Search lab used hard-coded container names.
  // Remove them proactively so reruns do not fail with name conflicts.
  removeContainerIfExists("mongod-community");
  removeContainerIfExists("mongot-community");
}

async function createStateFromOptions(options = {}, createOptions = {}) {
  const config = buildSearchConfig(options);

  const confirmed = createOptions.confirm === false
    ? true
    : await confirmAction(
      [
        "Create MongoDB Search lab with this configuration?",
        `Storage path: ${config.storagePath}`,
        `mongod image: ${config.mongodImage}`,
        `mongot image: ${config.mongotImage}`,
        `mongod port: ${config.mongodPort}`,
        `mongot port: ${config.mongotPort}`,
        `Metrics port: ${config.metricsPort}`
      ].join("\n"),
      true
    );

  if (!confirmed) {
    return null;
  }

  const files = await writeSearchFiles(config);
  const state = {
    projectName: config.projectName,
    config,
    ...files
  };

  await saveState(state);
  return state;
}

async function resolveStateForUp(options = {}) {
  let state = await loadState();
  const desiredConfig = buildSearchConfig(options);

  if (!state) {
    return createStateFromOptions(options, { confirm: options.confirm ?? true });
  }

  if (configsMatch(state.config, desiredConfig)) {
    console.log(`\nUsing existing Search lab config from ${state.config.storagePath}\n`);
    return state;
  }

  if (options.force) {
    await fs.rm(state.config.storagePath, { recursive: true, force: true });
    return createStateFromOptions(options, { confirm: false });
  }

  throw new Error(
    [
      `A different Search lab config already exists at ${state.config.storagePath}.`,
      "Run 'mongodb-cli-lab search clean' first or rerun the command with '--force'."
    ].join(" ")
  );
}

async function requireState() {
  const state = await loadState();

  if (!state) {
    throw new Error("No Search lab state found. Run 'mongodb-cli-lab search up' first.");
  }

  return state;
}

export async function runSearchUp(options = {}) {
  ensureDockerAvailable();
  const state = await resolveStateForUp(options);
  if (!state) {
    return null;
  }

  await ensureSearchAssets(state);
  await ensureSearchNetwork(state);
  cleanupLegacySearchContainers();
  await resolvePortConflicts(state, options);

  printStep(
    1,
    3,
    "Start mongod",
    "Launching the MongoDB Community Server service."
  );
  runCompose(state, ["up", "-d", "mongod"]);
  waitForMongoService(state, "mongod");
  ensureReplicaPrimary(state);
  ensureSearchCoordinatorUser(state);

  printStep(
    2,
    3,
    "Initialize mongod",
    "Configuring the replica set and creating the Search user."
  );
  ensureSearchCoordinatorUser(state);

  printStep(
    3,
    3,
    "Start mongot",
    "Launching the MongoDB Community Search service after mongod is ready."
  );
  runCompose(state, ["up", "-d", "mongot"]);
  if (!isServiceRunning(state, "mongot")) {
    throw new Error("mongot did not stay running after startup. Check 'mongodb-cli-lab search status' and logs.");
  }

  const hostConnectionString = `mongodb://localhost:${state.config.mongodPort}/?directConnection=true`;
  console.log("\nSearch lab ready\n");
  console.log(`mongod: ${hostConnectionString}`);
  console.log(`mongot gRPC: localhost:${state.config.mongotPort}\n`);
  return state;
}

export async function runSearchStatus() {
  ensureDockerAvailable();
  const state = await requireState();
  const containers = getComposeContainers(state);
  printStatus(state, containers);
}

export async function runSearchDown() {
  ensureDockerAvailable();
  const state = await requireState();
  console.log("\nStopping MongoDB Search lab...\n");
  runCompose(state, ["down"]);
}

export async function runSearchClean() {
  ensureDockerAvailable();
  const state = await requireState();

  console.log("\nRemoving MongoDB Search lab containers and generated files...\n");
  try {
    runCompose(state, ["down", "--remove-orphans", "-v"]);
  } catch {
    // Keep removing files even if Docker cleanup fails.
  }

  await deleteState(state.config.storagePath);
  await fs.rm(state.config.storagePath, { recursive: true, force: true });
}

export async function runSearchQuickstart(options = {}) {
  printQuickstartPlan(buildSearchConfig(options));
  const confirmed = await confirmAction("Proceed with MongoDB Search quickstart?", true);
  if (!confirmed) {
    console.log("\nSearch quickstart cancelled.\n");
    return;
  }

  const state = await runSearchUp({ ...options, confirm: false });
  if (!state) {
    return;
  }

  printStep(
    1,
    3,
    "Import sample data",
    "Importing the sample_mflix database used by the Search demo."
  );
  ensureSampleDatabasesRestored(state, [state.config.databaseName]);

  printStep(
    2,
    3,
    "Create Search index",
    "Creating the default Search index on sample_mflix.movies."
  );
  const indexResult = waitForSearchIndexManagement(state);

  printStep(
    3,
    3,
    "Run Search query",
    "Running a sample $search query for 'baseball'."
  );
  const documents = waitForSearchQuery(state);

  console.log("\nMongoDB Search quickstart completed\n");
  console.log(`Index action: ${indexResult.action}`);
  console.log(`Namespace: ${state.config.databaseName}.${state.config.collectionName}`);
  console.log(`Search index: ${state.config.searchIndexName}\n`);
  console.log("Sample results:\n");
  for (const document of documents) {
    console.log(JSON.stringify(document, null, 2));
  }
  console.log("\nTry this query in mongosh:\n");
  console.log(`use ${state.config.databaseName}`);
  console.log("db.movies.aggregate([");
  console.log("  {");
  console.log("    $search: {");
  console.log("      text: {");
  console.log('        query: "baseball",');
  console.log('        path: "plot"');
  console.log("      }");
  console.log("    }");
  console.log("  },");
  console.log("  { $limit: 5 },");
  console.log("  {");
  console.log("    $project: {");
  console.log("      _id: 0,");
  console.log("      title: 1,");
  console.log("      plot: 1");
  console.log("    }");
  console.log("  }");
  console.log("])\n");
  console.log(`Connect with: mongosh "mongodb://localhost:${state.config.mongodPort}/?directConnection=true"\n`);
}

export function getAvailableSampleDatabases() {
  return AVAILABLE_SAMPLE_DATABASES.map((database) => ({ ...database }));
}

export async function runSearchImportDatabases(options = {}) {
  ensureDockerAvailable();
  const state = await requireState();

  const requestedDatabases = options.all
    ? AVAILABLE_SAMPLE_DATABASES.map((database) => database.name)
    : options.databaseNames ?? [];

  if (!requestedDatabases.length) {
    throw new Error("Choose at least one sample database to import.");
  }

  ensureSampleArchiveExists(state);

  if (!isServiceRunning(state, "mongod")) {
    throw new Error("The Search lab is not running. Start it before importing sample databases.");
  }

  ensureReplicaPrimary(state);
  const counts = ensureSampleDatabasesRestored(state, requestedDatabases);

  console.log("\nImported sample databases\n");
  for (const database of counts) {
    console.log(`- ${database.name}.${database.collectionName}: ${database.count} documents`);
  }
  console.log("");
}
