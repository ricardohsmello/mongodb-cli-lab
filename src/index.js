#!/usr/bin/env node

import { existsSync } from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import inquirer from "inquirer";
import {
  buildQuickstartConfig,
  configsMatch,
  hasExplicitUpOptions,
  resolveUpConfig
} from "./lib/config.js";

const DEFAULT_STORAGE_PATH = "./mongodb-cli-lab";
const DEFAULT_PROJECT_NAME = "mongodb-cli-lab";
const COMPOSE_PROJECT_NAME = sanitizeProjectName(DEFAULT_PROJECT_NAME);
const INTERNAL_MONGO_PORT = 27017;
const STATE_FILE_NAME = ".mongodb-cli-lab-state.json";
const GLOBAL_STATE_DIR = path.join(os.homedir(), ".mongodb-cli-lab");
let dockerComposeCommand = null;
const DOCS = {
  sharding: "https://www.mongodb.com/docs/manual/sharding/",
  replication: "https://www.mongodb.com/docs/manual/replication/",
  shardKey: "https://www.mongodb.com/docs/manual/core/sharding-shard-key/",
  hashedSharding: "https://www.mongodb.com/docs/manual/core/hashed-sharding/",
  configServers: "https://www.mongodb.com/docs/manual/core/sharded-cluster-config-servers/",
  mongos: "https://www.mongodb.com/docs/manual/core/sharded-cluster-query-router/"
};
const BOOK_DEMO_DOCUMENTS = Object.freeze([
  { title: "Clean Code", author: "Robert C. Martin", year: 2008, genre: "software", pages: 464, isbn: "9780132350884" },
  { title: "Designing Data-Intensive Applications", author: "Martin Kleppmann", year: 2017, genre: "databases", pages: 616, isbn: "9781449373320" },
  { title: "Refactoring", author: "Martin Fowler", year: 1999, genre: "software", pages: 448, isbn: "9780201485677" },
  { title: "MongoDB: The Definitive Guide", author: "Kristina Chodorow", year: 2013, genre: "databases", pages: 432, isbn: "9781449344689" },
  { title: "Patterns of Enterprise Application Architecture", author: "Martin Fowler", year: 2002, genre: "architecture", pages: 560, isbn: "9780321127426" },
  { title: "Release It!", author: "Michael T. Nygard", year: 2007, genre: "operations", pages: 368, isbn: "9780978739218" },
  { title: "The Pragmatic Programmer", author: "Andrew Hunt", year: 1999, genre: "software", pages: 352, isbn: "9780201616224" },
  { title: "Building Microservices", author: "Sam Newman", year: 2015, genre: "architecture", pages: 280, isbn: "9781491950357" },
  { title: "Effective Java", author: "Joshua Bloch", year: 2018, genre: "software", pages: 416, isbn: "9780134685991" },
  { title: "Site Reliability Engineering", author: "Betsy Beyer", year: 2016, genre: "operations", pages: 552, isbn: "9781491929124" }
]);

function sleep(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function printDocsLink(label, url) {
  console.log(`Docs: ${label} -> ${url}`);
}

function buildTopology(config) {
  const configServers = Array.from({ length: config.configServerMembers }, (_, index) => ({
    id: `cfg${index + 1}`,
    serviceName: `cfg${index + 1}`,
    replicaSet: config.configServerReplicaSet,
    dbPath: path.join(config.storagePath, "configdb", `cfg${index + 1}`)
  }));

  const shards = Array.from({ length: config.shardCount }, (_, shardIndex) => {
    const shardId = shardIndex + 1;
    const replicaSet = `shardRS${shardId}`;
    const members = Array.from({ length: config.replicaSetMembers }, (_, memberIndex) => ({
      id: `shard${shardId}-${memberIndex + 1}`,
      serviceName: `shard${shardId}-${memberIndex + 1}`,
      replicaSet,
      dbPath: path.join(config.storagePath, `shard${shardId}`, `member${memberIndex + 1}`)
    }));

    return {
      id: `shard${shardId}`,
      replicaSet,
      members
    };
  });

  return {
    configServers,
    shards,
    mongos: {
      serviceName: "mongos",
      hostPort: config.mongosPort,
      containerPort: INTERNAL_MONGO_PORT
    }
  };
}

function indent(level, value) {
  return `${"  ".repeat(level)}${value}`;
}

function yamlQuote(value) {
  return `'${String(value).replaceAll("'", "''")}'`;
}

function serviceToYaml(service) {
  const lines = [
    indent(1, `${service.name}:`),
    indent(2, `image: mongo:${service.imageTag}`),
    indent(2, `container_name: ${service.containerName ?? service.name}`)
  ];

  if (service.dependsOn?.length) {
    lines.push(indent(2, "depends_on:"));
    for (const dependency of service.dependsOn) {
      lines.push(indent(3, `- ${dependency}`));
    }
  }

  lines.push(indent(2, "command:"));
  for (const part of service.command) {
    lines.push(indent(3, `- "${String(part).replaceAll('"', '\\"')}"`));
  }

  if (service.ports?.length) {
    lines.push(indent(2, "ports:"));
    for (const port of service.ports) {
      lines.push(indent(3, `- ${yamlQuote(port)}`));
    }
  }

  lines.push(indent(2, "volumes:"));
  for (const volume of service.volumes) {
    lines.push(indent(3, `- ${yamlQuote(volume)}`));
  }

  lines.push(indent(2, 'restart: "no"'));

  return lines.join("\n");
}

function generateComposeFile(config, topology) {
  const configServerConnection = topology.configServers
    .map((member) => `${member.serviceName}:${INTERNAL_MONGO_PORT}`)
    .join(",");

  const services = [
    ...topology.configServers.map((member) => ({
      name: member.serviceName,
      containerName: member.serviceName,
      imageTag: config.mongodbVersion,
      command: [
        "mongod",
        "--configsvr",
        "--replSet",
        member.replicaSet,
        "--bind_ip_all",
        "--port",
        INTERNAL_MONGO_PORT,
        "--dbpath",
        "/data/db"
      ],
      volumes: [`${member.dbPath}:/data/db`]
    })),
    ...topology.shards.flatMap((shard) =>
      shard.members.map((member) => ({
        name: member.serviceName,
        containerName: member.serviceName,
        imageTag: config.mongodbVersion,
        command: [
          "mongod",
          "--shardsvr",
          "--replSet",
          member.replicaSet,
          "--bind_ip_all",
          "--port",
          INTERNAL_MONGO_PORT,
          "--dbpath",
          "/data/db"
        ],
        volumes: [`${member.dbPath}:/data/db`]
      }))
    ),
    {
      name: topology.mongos.serviceName,
      containerName: topology.mongos.serviceName,
      imageTag: config.mongodbVersion,
      dependsOn: topology.configServers.map((member) => member.serviceName),
      command: [
        "mongos",
        "--configdb",
        `${config.configServerReplicaSet}/${configServerConnection}`,
        "--bind_ip_all",
        "--port",
        INTERNAL_MONGO_PORT
      ],
      ports: [`${topology.mongos.hostPort}:${topology.mongos.containerPort}`],
      volumes: [path.join(config.storagePath, "logs") + ":/var/log/mongodb"]
    }
  ];

  return ["services:", ...services.map(serviceToYaml)].join("\n");
}

function buildReplicaSetConfig(replicaSet, members, options = {}) {
  return {
    _id: replicaSet,
    ...(options.configsvr ? { configsvr: true } : {}),
    members: members.map((member, index) => ({
      _id: index,
      host: `${member.serviceName}:${INTERNAL_MONGO_PORT}`
    }))
  };
}

function buildReplicaInitScript(replicaConfig) {
  return `
const cfg = ${JSON.stringify(replicaConfig, null, 2)};
try {
  const status = db.adminCommand({ replSetGetStatus: 1 });
  if (status.ok === 1) {
    print("Replica set already initialized: " + status.set);
    quit(0);
  }
} catch (error) {
  if (error.code !== 94) {
    throw error;
  }
}

print("Initializing replica set " + cfg._id);
const initiateResult = rs.initiate(cfg);
printjson(initiateResult);

for (let attempt = 0; attempt < 120; attempt += 1) {
  try {
    const hello = (db.hello && db.hello()) || db.isMaster();
    if (hello.isWritablePrimary || hello.ismaster === true) {
      print("Primary ready for " + cfg._id);
      quit(0);
    }

    const status = db.adminCommand({ replSetGetStatus: 1 });
    if (status.ok === 1) {
      const members = (status.members || []).map((member) => ({
        name: member.name,
        state: member.stateStr,
        health: member.health
      }));

      if (attempt === 0 || attempt % 5 === 0) {
        print(
          "Waiting for primary in " +
            cfg._id +
            " (attempt " +
            (attempt + 1) +
            "/120): " +
            JSON.stringify(members)
        );
      }
    } else if (attempt === 0 || attempt % 5 === 0) {
      print(
        "Waiting for primary in " +
          cfg._id +
          " (attempt " +
          (attempt + 1) +
          "/120), status not ready yet"
      );
    }
  } catch (error) {
    if (attempt === 0 || attempt % 5 === 0) {
      print(
        "Waiting for primary in " +
          cfg._id +
          " (attempt " +
          (attempt + 1) +
          "/120), reason: " +
          error.message
      );
    }
  }

  sleep(1000);
}

try {
  const finalStatus = db.adminCommand({ replSetGetStatus: 1 });
  print("Final replica set status for " + cfg._id + ":");
  printjson(finalStatus);
} catch (error) {
  print("Could not read final replica set status for " + cfg._id + ": " + error.message);
}

throw new Error("Timeout waiting for primary in " + cfg._id);
`.trim();
}

function buildAddShardsScript(topology) {
  const shardConnections = topology.shards.map(
    (shard) =>
      `${shard.replicaSet}/${shard.members
        .map((member) => `${member.serviceName}:${INTERNAL_MONGO_PORT}`)
        .join(",")}`
  );

  return `
const shardConnections = ${JSON.stringify(shardConnections, null, 2)};

for (const connectionString of shardConnections) {
  const shardName = connectionString.split("/")[0];
  const current = db.adminCommand({ listShards: 1 });
  if (current.ok !== 1) {
    throw new Error("listShards failed: " + tojson(current));
  }

  const alreadyAdded = (current.shards || []).some((shard) => shard._id === shardName);
  if (alreadyAdded) {
    print("Shard already exists: " + connectionString);
    continue;
  }

  const result = sh.addShard(connectionString);
  if (result.ok !== 1) {
    throw new Error("addShard failed: " + tojson(result));
  }

  print("Added shard: " + connectionString);
}

sh.status();
`.trim();
}

async function ensureDirectories(topology, storagePath) {
  const directories = [
    path.join(storagePath, "configdb"),
    path.join(storagePath, "logs"),
    ...topology.configServers.map((member) => member.dbPath),
    ...topology.shards.flatMap((shard) => [
      path.join(storagePath, shard.id),
      ...shard.members.map((member) => member.dbPath)
    ])
  ];

  await Promise.all(directories.map((directory) => fs.mkdir(directory, { recursive: true })));
}

function runCommand(command, args, options = {}) {
  return execFileSync(command, args, {
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    encoding: options.capture ? "utf8" : undefined
  });
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

async function writeClusterFiles(config, topology) {
  await ensureDirectories(topology, config.storagePath);

  const composeFile = path.join(config.storagePath, "docker-compose.yml");
  const clusterConfigFile = path.join(config.storagePath, "cluster-config.json");
  const topologyFile = path.join(config.storagePath, "topology.json");

  await fs.writeFile(composeFile, generateComposeFile(config, topology), "utf8");
  await fs.writeFile(clusterConfigFile, JSON.stringify(config, null, 2), "utf8");
  await fs.writeFile(topologyFile, JSON.stringify(topology, null, 2), "utf8");

  return {
    composeFile,
    clusterConfigFile,
    topologyFile
  };
}

function getStateFilePath(storagePath) {
  return path.join(storagePath, STATE_FILE_NAME);
}

function getGlobalStateFilePath() {
  return path.join(GLOBAL_STATE_DIR, STATE_FILE_NAME);
}

async function saveState(state) {
  await fs.mkdir(GLOBAL_STATE_DIR, { recursive: true });
  await fs.writeFile(getGlobalStateFilePath(), JSON.stringify(state, null, 2), "utf8");
  await fs.writeFile(getStateFilePath(state.config.storagePath), JSON.stringify(state, null, 2), "utf8");
}

async function tryReadStateFile(stateFilePath) {
  if (!existsSync(stateFilePath)) {
    return null;
  }

  const raw = await fs.readFile(stateFilePath, "utf8");
  return JSON.parse(raw);
}

function tryRunCommand(command, args, options = {}) {
  try {
    return runCommand(command, args, options);
  } catch {
    return null;
  }
}

function listProjectContainerIds(serviceName = null) {
  const args = [
    "ps",
    "-a",
    "--filter",
    `label=com.docker.compose.project=${COMPOSE_PROJECT_NAME}`
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

function inferStoragePathFromContainerInspect(containerDetails) {
  const mounts = Array.isArray(containerDetails?.Mounts) ? containerDetails.Mounts : [];

  for (const mount of mounts) {
    if (typeof mount?.Source !== "string") {
      continue;
    }

    if (mount.Source.endsWith(`${path.sep}logs`)) {
      return path.dirname(mount.Source);
    }

    if (mount.Source.endsWith(`${path.sep}configdb${path.sep}cfg1`)) {
      return path.dirname(path.dirname(mount.Source));
    }
  }

  return null;
}

async function loadStateFromStoragePath(storagePath) {
  const clusterConfigFile = path.join(storagePath, "cluster-config.json");
  const topologyFile = path.join(storagePath, "topology.json");
  const composeFile = path.join(storagePath, "docker-compose.yml");

  if (!existsSync(clusterConfigFile) || !existsSync(topologyFile) || !existsSync(composeFile)) {
    return null;
  }

  const [configRaw, topologyRaw] = await Promise.all([
    fs.readFile(clusterConfigFile, "utf8"),
    fs.readFile(topologyFile, "utf8")
  ]);

  return {
    projectName: sanitizeProjectName(DEFAULT_PROJECT_NAME),
    config: JSON.parse(configRaw),
    topology: JSON.parse(topologyRaw),
    composeFile,
    clusterConfigFile,
    topologyFile
  };
}

async function discoverStateFromDocker() {
  const containerIds = [
    ...listProjectContainerIds("mongos"),
    ...listProjectContainerIds()
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

  const containerDetails = Array.isArray(details) ? details[0] : null;
  const storagePath = inferStoragePathFromContainerInspect(containerDetails);
  if (!storagePath) {
    return null;
  }

  const discoveredState = await loadStateFromStoragePath(storagePath);
  if (!discoveredState) {
    return null;
  }

  await saveState(discoveredState);
  return discoveredState;
}

async function loadActiveClusterState() {
  return discoverStateFromDocker();
}

async function loadState() {
  const activeState = await loadActiveClusterState();
  if (activeState) {
    return activeState;
  }

  const globalState = await tryReadStateFile(getGlobalStateFilePath());
  if (globalState) {
    return globalState;
  }

  const legacyStatePath = getStateFilePath(path.resolve(process.cwd(), DEFAULT_STORAGE_PATH));
  const legacyState = await tryReadStateFile(legacyStatePath);
  if (!legacyState) {
    return null;
  }

  await saveState(legacyState);
  return legacyState;
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

function countExpectedNodes(state) {
  return (
    state.topology.configServers.length +
    state.topology.shards.reduce((total, shard) => total + shard.members.length, 0) +
    1
  );
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

function summarizeServiceRole(serviceName) {
  if (serviceName.startsWith("cfg")) {
    return "configsvr";
  }

  if (serviceName === "mongos") {
    return "mongos";
  }

  return "shardsvr";
}

function buildDisplayName(serviceName, port = INTERNAL_MONGO_PORT) {
  if (serviceName.startsWith("cfg")) {
    return `cfg-${port}`;
  }

  if (serviceName === "mongos") {
    return `mongos-${port}`;
  }

  return `shard-${port}`;
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

function buildContainerMap(containers) {
  return new Map(containers.map((container) => [container.Service, container]));
}

function containerStateLabel(container) {
  return container?.State ?? "not created";
}

function containerPortLabel(container, fallbackPort = INTERNAL_MONGO_PORT) {
  const ports = formatPorts(container?.Publishers);
  if (ports !== "-") {
    return ports;
  }

  return `internal:${fallbackPort}`;
}

function printClusterSummary(state, containers) {
  const runningCount = containers.filter((container) => container.State === "running").length;
  const expectedNodes = countExpectedNodes(state);

  console.log("\nCluster summary\n");
  console.log(`Project: ${state.projectName}`);
  console.log(`Storage: ${state.config.storagePath}`);
  console.log(`MongoDB version: ${state.config.mongodbVersion}`);
  console.log(`Shards: ${state.config.shardCount}`);
  console.log(`Replica set members per shard: ${state.config.replicaSetMembers}`);
  console.log(`Nodes running: ${runningCount}/${expectedNodes}`);
  console.log(`mongos: mongodb://localhost:${state.config.mongosPort}\n`);
}

function printContainersTable(containers) {
  if (!containers.length) {
    console.log("No containers found for this cluster.\n");
    return;
  }

  const rows = containers.map((container) => ({
    service: buildDisplayName(container.Service),
    container: container.Service,
    role: summarizeServiceRole(container.Service),
    state: container.State,
    ports: formatPorts(container.Publishers)
  }));

  console.table(rows);
}

function printTopologyDetails(state, containers) {
  const containerMap = buildContainerMap(containers);

  console.log("Topology\n");

  console.log(`Config server replica set: ${state.config.configServerReplicaSet}`);
  console.log(`Members: ${state.config.configServerMembers}`);
  for (const member of state.topology.configServers) {
    const container = containerMap.get(member.serviceName);
    console.log(
      `- ${buildDisplayName(member.serviceName)} (${member.serviceName}) | state: ${containerStateLabel(container)} | port: ${containerPortLabel(container)}`
    );
  }

  console.log(`\nRouter`);
  const mongosContainer = containerMap.get(state.topology.mongos.serviceName);
  console.log(
    `- ${buildDisplayName(state.topology.mongos.serviceName)} (${state.topology.mongos.serviceName}) | state: ${containerStateLabel(mongosContainer)} | port: ${containerPortLabel(mongosContainer)}`
  );

  console.log(`\nShards`);
  for (const [index, shard] of state.topology.shards.entries()) {
    console.log(`- Shard ${index + 1}: ${shard.replicaSet}`);
    console.log(`  Members: ${shard.members.length}`);
    for (const member of shard.members) {
      const container = containerMap.get(member.serviceName);
      console.log(
        `  - ${buildDisplayName(member.serviceName)} (${member.serviceName}) | state: ${containerStateLabel(container)} | port: ${containerPortLabel(container)}`
      );
    }
  }

  console.log();
}

function printReplicaSetHealth(state, containers) {
  const containerMap = buildContainerMap(containers);
  const configRunning = state.topology.configServers.filter(
    (member) => containerMap.get(member.serviceName)?.State === "running"
  ).length;

  console.log("Replica set health\n");
  console.log(
    `- ${state.config.configServerReplicaSet} | running members: ${configRunning}/${state.topology.configServers.length}`
  );

  for (const shard of state.topology.shards) {
    const runningMembers = shard.members.filter(
      (member) => containerMap.get(member.serviceName)?.State === "running"
    ).length;

    const label =
      runningMembers === shard.members.length
        ? "healthy"
        : runningMembers === 0
          ? "down"
          : "degraded";

    console.log(
      `- ${shard.replicaSet} | ${label} | running members: ${runningMembers}/${shard.members.length}`
    );
  }

  const mongosState = containerMap.get(state.topology.mongos.serviceName)?.State ?? "not created";
  console.log(`- mongos | state: ${mongosState}\n`);
}

function printTopologyDiagram(state) {
  console.log("Cluster structure\n");
  console.log("        +---------------------------+");
  console.log(`        | mongos                    |`);
  console.log(`        | localhost:${state.config.mongosPort}${" ".repeat(Math.max(0, 12 - String(state.config.mongosPort).length))}|`);
  console.log("        +-------------+-------------+");
  console.log("                      |");
  console.log("        +-------------v-------------+");
  console.log(`        | ${state.config.configServerReplicaSet.padEnd(27, " ")}|`);
  console.log(
    `        | ${state.topology.configServers.map((member) => member.serviceName).join(" ").padEnd(27, " ")}|`
  );
  console.log("        +-------------+-------------+");
  console.log("                      |");

  for (const shard of state.topology.shards) {
    console.log("               +------v------+");
    console.log(`               | ${shard.replicaSet.padEnd(11, " ")}|`);
    console.log(
      `               | ${`${shard.members.length} members`.padEnd(11, " ")}|`
    );
    console.log("               +-------------+");
  }

  console.log();
}

function printStep(stepNumber, totalSteps, title, description) {
  console.log(`\n=== Step ${stepNumber}/${totalSteps}: ${title} ===`);
  if (description) {
    console.log(description);
  }
}

function waitForMongo(state, serviceName) {
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

function waitForServices(state, serviceNames) {
  for (const serviceName of serviceNames) {
    waitForMongo(state, serviceName);
  }
}

function runMongoScript(state, serviceName, script) {
  runCompose(state, ["exec", "-T", serviceName, "mongosh", "--quiet", "--eval", script]);
}

function runMongoScriptCapture(state, serviceName, script) {
  return runCompose(
    state,
    ["exec", "-T", serviceName, "mongosh", "--quiet", "--eval", script],
    { capture: true }
  );
}

function runMongoJson(state, serviceName, script) {
  const marker = "__MONGO_SHARDING_LAB_JSON__";
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
    throw new Error("Could not parse MongoDB command output.");
  }

  return JSON.parse(line.slice(marker.length));
}

function getCollectionDocumentCount(state, databaseName, collectionName) {
  return runMongoJson(
    state,
    state.topology.mongos.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const collection = db.getSiblingDB(databaseName).getCollection(collectionName);

let count = 0;
try {
  count = collection.countDocuments({});
} catch (error) {
  if (error.codeName !== "NamespaceNotFound" && !String(error.message || "").includes("ns does not exist")) {
    throw error;
  }
}

const result = { count };
`.trim()
  ).count;
}

function getClusterOverview(state) {
  return runMongoJson(
    state,
    state.topology.mongos.serviceName,
    `
const userDatabases = db
  .adminCommand({ listDatabases: 1 })
  .databases
  .map((database) => database.name)
  .filter((name) => !["admin", "config", "local"].includes(name));

const shardedCollections = new Map(
  db
    .getSiblingDB("config")
    .collections
    .find(
      {
        dropped: { $ne: true },
        key: { $exists: true }
      },
      { _id: 1, key: 1 }
    )
    .toArray()
    .map((collection) => [collection._id, collection.key])
);

const collections = [];
for (const databaseName of userDatabases) {
  const infos = db
    .getSiblingDB(databaseName)
    .getCollectionInfos({}, true)
    .filter((info) => !info.name.startsWith("system."));

  for (const info of infos) {
    const namespace = databaseName + "." + info.name;
    collections.push({
      db: databaseName,
      name: info.name,
      namespace,
      sharded: shardedCollections.has(namespace),
      shardKey: shardedCollections.get(namespace) || null
    });
  }
}

const result = {
  databases: userDatabases,
  collections
};
`.trim()
  );
}

function printCollectionOverview(overview) {
  if (!overview.collections.length) {
    console.log("\nNo user collections found in the cluster.\n");
    return;
  }

  console.log("\nCollections\n");
  for (const collection of overview.collections) {
    const shardLabel = collection.sharded
      ? `sharded by ${JSON.stringify(collection.shardKey)}`
      : "not sharded";
    console.log(`- ${collection.namespace} | ${shardLabel}`);
  }
  console.log();
}

function printBooksDemoIntroduction() {
  console.log("\nGuided demo: library.books\n");
  printDocsLink("Shard keys", DOCS.shardKey);
  printDocsLink("Hashed sharding", DOCS.hashedSharding);
  console.log();
  console.log("Sample document:");
  console.log(
    JSON.stringify(
      {
        title: "Clean Code",
        author: "Robert C. Martin",
        year: 2008,
        genre: "software",
        pages: 464,
        isbn: "9780132350884"
      },
      null,
      2
    )
  );
  console.log("\nThis demo creates a 'library.books' collection and shards it by a chosen field.\n");
  console.log("After sharding and inserts, the CLI will show how documents were distributed across shards.\n");
}

function ensureMongosRunning(state) {
  const containers = getComposeContainers(state);
  const mongos = containers.find((container) => container.Service === state.topology.mongos.serviceName);

  if (!mongos || mongos.State !== "running") {
    throw new Error("mongos is not running. Start the cluster before managing sharded collections.");
  }
}

async function promptCollectionTarget(overview) {
  const { databaseAction } = await inquirer.prompt([
      {
        type: "list",
        name: "databaseAction",
        message: "Step 1: Choose a database for this sharding exercise",
      choices: [
        ...overview.databases.map((database) => ({
          name: database,
          value: { mode: "existing", database }
        })),
        { name: "Create new database", value: { mode: "new" } },
        { name: "Back", value: { mode: "back" } }
      ]
    }
  ]);

  if (databaseAction.mode === "back") {
    return null;
  }

  let databaseName = databaseAction.database;
  if (databaseAction.mode === "new") {
    const answer = await inquirer.prompt([
      {
        type: "input",
        name: "databaseName",
        message: "Database name:",
        validate: (value) =>
          value.trim().toLowerCase() === "back" || value.trim()
            ? true
            : "Database name cannot be empty."
      }
    ]);

    if (answer.databaseName.trim().toLowerCase() === "back") {
      return null;
    }

    databaseName = answer.databaseName.trim();
  }

  const collectionsInDatabase = overview.collections.filter(
    (collection) => collection.db === databaseName
  );

  const { collectionAction } = await inquirer.prompt([
      {
        type: "list",
        name: "collectionAction",
        message: "Step 2: Choose a collection inside that database",
      choices: [
        ...collectionsInDatabase.map((collection) => ({
          name: `${collection.name}${collection.sharded ? " (already sharded)" : ""}`,
          value: { mode: "existing", collection }
        })),
        { name: "Create new collection", value: { mode: "new" } },
        { name: "Back", value: { mode: "back" } }
      ]
    }
  ]);

  if (collectionAction.mode === "back") {
    return null;
  }

  let collectionName = collectionAction.collection?.name;
  let alreadySharded = collectionAction.collection?.sharded ?? false;
  if (collectionAction.mode === "new") {
    const answer = await inquirer.prompt([
      {
        type: "input",
        name: "collectionName",
        message: "Collection name:",
        validate: (value) =>
          value.trim().toLowerCase() === "back" || value.trim()
            ? true
            : "Collection name cannot be empty."
      }
    ]);

    if (answer.collectionName.trim().toLowerCase() === "back") {
      return null;
    }

    collectionName = answer.collectionName.trim();
    alreadySharded = false;
  }

  return {
    databaseName,
    collectionName,
    alreadySharded,
    currentShardKey: collectionAction.collection?.shardKey ?? null
  };
}

function shardCollection(state, options) {
  const {
    databaseName,
    collectionName,
    shardKeyField,
    shardKeyMode = "range",
    documents,
    resetCollection = false,
    skipInsert = false
  } = options;

  return runMongoJson(
    state,
    state.topology.mongos.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const shardKeyField = ${JSON.stringify(shardKeyField)};
const shardKeyMode = ${JSON.stringify(shardKeyMode)};
const documents = ${JSON.stringify(documents)};
const resetCollection = ${JSON.stringify(resetCollection)};
const skipInsert = ${JSON.stringify(skipInsert)};
const namespace = databaseName + "." + collectionName;
const database = db.getSiblingDB(databaseName);
const shardKey = { [shardKeyField]: shardKeyMode === "hashed" ? "hashed" : 1 };

if (resetCollection) {
  try {
    database.getCollection(collectionName).drop();
  } catch (error) {
    if (error.codeName !== "NamespaceNotFound") {
      throw error;
    }
  }
}

try {
  database.createCollection(collectionName);
} catch (error) {
  if (error.codeName !== "NamespaceExists") {
    throw error;
  }
}

const enableResult = sh.enableSharding(databaseName);
let shardResult;
let actionTaken;
const existing = db.getSiblingDB("config").collections.findOne({ _id: namespace, dropped: { $ne: true } });
if (existing && existing.key) {
  if (JSON.stringify(existing.key) === JSON.stringify(shardKey)) {
    shardResult = { ok: 1, note: "already sharded with requested key", key: existing.key };
    actionTaken = "reuse-existing-shard-key";
  } else {
    shardResult = db.adminCommand({
      reshardCollection: namespace,
      key: shardKey
    });
    actionTaken = "reshard-collection";
  }
} else {
  shardResult = sh.shardCollection(namespace, shardKey);
  actionTaken = "initial-shard-collection";
}

let inserted = 0;
if (!skipInsert && documents.length > 0) {
  const insertResult = database.getCollection(collectionName).insertMany(documents, { ordered: false });
  inserted = insertResult.insertedIds ? Object.keys(insertResult.insertedIds).length : documents.length;
}

const result = {
  databaseName,
  collectionName,
  namespace,
  shardKey,
  previousShardKey: existing?.key || null,
  actionTaken,
  enableResult,
  shardResult,
  inserted
};
`.trim()
  );
}

function printInsertPlan(label, insertCount) {
  if (insertCount <= 0) {
    console.log(`No demo documents will be inserted for ${label}.\n`);
    return;
  }

  console.log(`Preparing to generate and insert ${insertCount} documents for ${label}...`);
  if (insertCount >= 10000) {
    console.log("This may take a while depending on your machine and Docker performance.");
  }
  console.log();
}

function formatShardKey(field, mode = "range") {
  return JSON.stringify({ [field]: mode === "hashed" ? "hashed" : 1 });
}

function getRecommendedBatchSize(insertCount) {
  if (insertCount >= 1000000) {
    return 50000;
  }

  if (insertCount >= 100000) {
    return 20000;
  }

  if (insertCount >= 10000) {
    return 10000;
  }

  return 5000;
}

function insertDocumentsInBatches(state, options) {
  const {
    databaseName,
    collectionName,
    shardKeyField,
    insertCount,
    startIndex = 0,
    seedMode,
    batchSize = getRecommendedBatchSize(insertCount)
  } = options;

  let inserted = 0;
  console.log(`Using batch size: ${batchSize}`);

  for (let offset = 0; offset < insertCount; offset += batchSize) {
    const currentBatchSize = Math.min(batchSize, insertCount - offset);
    const batchStartIndex = startIndex + offset;
    const batchResult = runMongoJson(
      state,
      state.topology.mongos.serviceName,
      `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const shardKeyField = ${JSON.stringify(shardKeyField)};
const seedMode = ${JSON.stringify(seedMode)};
const currentBatchSize = ${currentBatchSize};
const batchStartIndex = ${batchStartIndex};
const database = db.getSiblingDB(databaseName);

function buildSampleDocuments(fieldName, totalCount, initialOffset) {
  const generated = [];
  for (let relativeIndex = 1; relativeIndex <= totalCount; relativeIndex += 1) {
    const index = initialOffset + relativeIndex;
    generated.push({
      [fieldName]: fieldName === "_id" ? index : fieldName + "-" + index,
      sampleIndex: index,
      source: "mongodb-cli-lab",
      createdAt: new Date()
    });
  }
  return generated;
}

function buildBooksDataset(totalCount, initialOffset) {
  const seedBooks = ${JSON.stringify(BOOK_DEMO_DOCUMENTS)};

  const generated = [];
  for (let relativeIndex = 0; relativeIndex < totalCount; relativeIndex += 1) {
    const index = initialOffset + relativeIndex;
    const base = seedBooks[index % seedBooks.length];
    generated.push({
      _id: index + 1,
      title: base.title + " #" + (index + 1),
      author: base.author,
      year: base.year + (index % 5),
      genre: base.genre,
      pages: base.pages + (index % 20),
      isbn: base.isbn + "-" + (index + 1),
      copyNumber: index + 1
    });
  }
  return generated;
}

const documents =
  seedMode === "books-demo"
    ? buildBooksDataset(currentBatchSize, batchStartIndex)
    : buildSampleDocuments(shardKeyField, currentBatchSize, batchStartIndex);

const insertResult = database.getCollection(collectionName).insertMany(documents, { ordered: false });
const result = {
  inserted: insertResult.insertedIds ? Object.keys(insertResult.insertedIds).length : documents.length
};
`.trim()
    );

    inserted += batchResult.inserted;
    console.log(`Inserted ${inserted}/${insertCount} documents...`);
  }

  console.log();
  return inserted;
}

function buildSampleDocuments(shardKeyField, insertCount, startIndex = 0) {
  const documents = [];
  for (let relativeIndex = 1; relativeIndex <= insertCount; relativeIndex += 1) {
    const index = startIndex + relativeIndex;
    documents.push({
      [shardKeyField]: shardKeyField === "_id" ? index : `${shardKeyField}-${index}`,
      sampleIndex: index,
      source: "mongodb-cli-lab",
      createdAt: new Date().toISOString()
    });
  }

  return documents;
}

function buildBooksDemoDocuments() {
  return BOOK_DEMO_DOCUMENTS;
}

function getCollectionDistribution(state, databaseName, collectionName) {
  return runMongoJson(
    state,
    state.topology.mongos.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const namespace = databaseName + "." + collectionName;
const stats = db.getSiblingDB(databaseName).runCommand({ collStats: collectionName });

const shardStats = Object.entries(stats.shards || {}).map(([shardName, shardInfo]) => ({
  shard: shardName,
  count: shardInfo.count ?? 0,
  size: shardInfo.size ?? 0,
  storageSize: shardInfo.storageSize ?? 0
}));

const chunkInfo = db
  .getSiblingDB("config")
  .chunks
  .aggregate([
    { $match: { ns: namespace } },
    { $group: { _id: "$shard", chunks: { $sum: 1 } } },
    { $sort: { _id: 1 } }
  ])
  .toArray()
  .map((entry) => ({ shard: entry._id, chunks: entry.chunks }));

const chunkSamples = db
  .getSiblingDB("config")
  .chunks
  .find(
    { ns: namespace },
    { shard: 1, min: 1, max: 1 }
  )
  .limit(12)
  .toArray()
  .map((chunk) => ({
    shard: chunk.shard,
    min: chunk.min,
    max: chunk.max
  }));

const result = {
  namespace,
  count: stats.count ?? 0,
  shardStats,
  chunkInfo,
  chunkSamples
};
`.trim()
  );
}

function buildDistributionInsight(distribution) {
  if (!distribution.shardStats.length) {
    return "No shard-level distribution is available yet.";
  }

  const sorted = [...distribution.shardStats].sort((left, right) => right.count - left.count);
  const top = sorted[0];
  const total = distribution.count || 0;
  const topRatio = total > 0 ? top.count / total : 0;
  const activeShards = sorted.filter((entry) => entry.count > 0).length;

  if (activeShards <= 1 && total > 0) {
    return [
      `All documents are currently on ${top.shard}.`,
      "This usually means the shard key is concentrating values into a small number of chunks, or the balancer has not had enough time/work to redistribute data yet."
    ].join(" ");
  }

  if (topRatio >= 0.75) {
    return [
      `${top.shard} currently holds most of the documents (${Math.round(topRatio * 100)}%).`,
      "This is a useful sign that the chosen shard key may not be spreading values evenly."
    ].join(" ");
  }

  return "Documents are distributed across multiple shards. Balance is influenced by shard key choice, chunk splits, and balancer timing.";
}

function printCollectionDistribution(distribution) {
  console.log("Distribution\n");
  console.log(`Namespace: ${distribution.namespace}`);
  console.log(`Total documents: ${distribution.count}`);

  if (!distribution.shardStats.length) {
    console.log("No per-shard stats available yet.\n");
    return;
  }

  for (const shard of distribution.shardStats) {
    const chunk = distribution.chunkInfo.find((entry) => entry.shard === shard.shard);
    console.log(
      `- ${shard.shard} | documents: ${shard.count} | chunks: ${chunk?.chunks ?? 0}`
    );
  }

  if (distribution.chunkSamples?.length) {
    console.log("\nChunk samples");
    for (const chunk of distribution.chunkSamples) {
      console.log(
        `- ${chunk.shard} | min: ${JSON.stringify(chunk.min)} | max: ${JSON.stringify(chunk.max)}`
      );
    }
  }

  console.log(`\nInsight: ${buildDistributionInsight(distribution)}`);
  console.log(
    "\nNote: distribution is not guaranteed to be perfectly even. It depends on the shard key, value spread, chunk splits, and balancer timing.\n"
  );
}

function getAvailableShardMemberServiceName(state, shardReplicaSet) {
  const shard = state.topology.shards.find((entry) => entry.replicaSet === shardReplicaSet);
  if (!shard) {
    return null;
  }

  const containerMap = buildContainerMap(getComposeContainers(state));
  const runningMember = shard.members.find(
    (member) => containerMap.get(member.serviceName)?.State === "running"
  );

  return runningMember?.serviceName ?? shard.members[0]?.serviceName ?? null;
}

function inspectShardCollection(state, databaseName, collectionName, shardReplicaSet) {
  const serviceName = getAvailableShardMemberServiceName(state, shardReplicaSet);
  if (!serviceName) {
    throw new Error(`Could not find a container for shard '${shardReplicaSet}'.`);
  }

  return runMongoJson(
    state,
    serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const database = db.getSiblingDB(databaseName);
const collection = database.getCollection(collectionName);
let count = 0;
let sampleDocuments = [];
let indexes = [];

try {
  count = collection.countDocuments({});
  sampleDocuments = collection.find({}).limit(5).toArray();
  indexes = collection.getIndexes().map((index) => ({ name: index.name, key: index.key }));
} catch (error) {
  if (error.codeName !== "NamespaceNotFound" && !String(error.message || "").includes("ns does not exist")) {
    throw error;
  }
}

const result = {
  shard: ${JSON.stringify(shardReplicaSet)},
  databaseName,
  collectionName,
  count,
  sampleDocuments,
  indexes
};
`.trim()
  );
}

function printShardInspection(inspection) {
  console.log("\nShard inspection\n");
  console.log(`Shard: ${inspection.shard}`);
  console.log(`Namespace: ${inspection.databaseName}.${inspection.collectionName}`);
  console.log(`Documents visible on this shard primary: ${inspection.count}`);
  console.log(`Indexes: ${JSON.stringify(inspection.indexes)}\n`);

  if (!inspection.sampleDocuments.length) {
    console.log("No documents found on this shard for the selected collection.\n");
    return;
  }

  console.log("Sample documents on this shard:\n");
  for (const document of inspection.sampleDocuments) {
    console.log(JSON.stringify(document, null, 2));
  }
  console.log();
}

function getShardDataPresence(state, databaseName, collectionName) {
  return state.topology.shards.map((shard) => {
    try {
      const inspection = inspectShardCollection(
        state,
        databaseName,
        collectionName,
        shard.replicaSet
      );

      return {
        shard: shard.replicaSet,
        count: inspection.count,
        available: true
      };
    } catch (error) {
      return {
        shard: shard.replicaSet,
        count: 0,
        available: false,
        error: error.message
      };
    }
  });
}

function printShardDataPresence(databaseName, collectionName, shardPresence) {
  console.log("\nShard data presence\n");
  console.log(`Namespace: ${databaseName}.${collectionName}\n`);

  for (const entry of shardPresence) {
    if (entry.available === false) {
      console.log(`- ${entry.shard} | unavailable | ${entry.error}`);
      continue;
    }

    const status = entry.count > 0 ? "has data" : "no data";
    console.log(`- ${entry.shard} | ${status} | documents: ${entry.count}`);
  }

  const availableShards = shardPresence.filter((entry) => entry.available !== false);
  const shardsWithData = availableShards.filter((entry) => entry.count > 0).length;
  const totalShards = availableShards.length;

  if (!availableShards.length) {
    console.log("\nNo shard members are currently available for inspection.\n");
    return;
  }

  if (shardsWithData === 0) {
    console.log("\nNo shard currently reports local documents for this collection.\n");
    return;
  }

  if (shardsWithData === 1) {
    console.log(
      "\nOnly one shard currently has data. This usually means the collection has not balanced yet, or the shard key is concentrating writes into one shard.\n"
    );
    return;
  }

  if (shardsWithData < totalShards) {
    console.log(
      "\nSome shards have data and others do not. Distribution has started, but it is not spread across the whole cluster yet.\n"
    );
    return;
  }

  console.log("\nAll shards currently have at least some data for this collection.\n");
}

function printQuickstartPlan(config) {
  console.log("\nQuickstart mode\n");
  console.log("This command will:");
  console.log(`- create a ${config.shardCount}-shard cluster`);
  console.log(`- use ${config.replicaSetMembers} replica set members per shard`);
  console.log(`- run MongoDB ${config.mongodbVersion}`);
  console.log(`- expose mongos on localhost:${config.mongosPort}`);
  console.log("- create the demo collection library.books");
  console.log('- shard the collection by { "_id": "hashed" }');
  console.log("- insert 500 sample documents");
  console.log("- show how documents were distributed across shards\n");
}

function printQuickstartSummary(result, inserted, distribution) {
  console.log("\nQuickstart demo completed\n");
  console.log(`Namespace: ${result.namespace}`);
  console.log(`Shard key: ${JSON.stringify(result.shardKey)}`);
  console.log(`Action: ${result.actionTaken}`);
  console.log(`Documents inserted: ${inserted}\n`);
  printCollectionDistribution(distribution);
  console.log("Try next:");
  console.log("- mongodb-cli-lab status");
  console.log("- mongodb-cli-lab");
  console.log();
}

async function runQuickstartDemo(state) {
  const databaseName = "library";
  const collectionName = "books";
  const insertCount = 500;

  printStep(
    1,
    3,
    "Create demo collection",
    "Preparing a small sharded collection so the cluster is immediately useful for learning."
  );
  const result = shardCollection(state, {
    databaseName,
    collectionName,
    shardKeyField: "_id",
    shardKeyMode: "hashed",
    documents: [],
    resetCollection: true,
    skipInsert: true
  });

  printStep(
    2,
    3,
    "Insert sample documents",
    "Writing 500 example documents into library.books."
  );
  printInsertPlan(`${databaseName}.${collectionName}`, insertCount);
  const inserted = insertDocumentsInBatches(state, {
    databaseName,
    collectionName,
    shardKeyField: "_id",
    insertCount,
    seedMode: "books-demo"
  });

  printStep(
    3,
    3,
    "Inspect distribution",
    "Checking how MongoDB distributed the documents across shards."
  );
  const distribution = getCollectionDistribution(state, databaseName, collectionName);
  printQuickstartSummary(result, inserted, distribution);
}

async function promptExistingCollection(overview) {
  if (!overview.collections.length) {
    console.log("\nNo user collections found in the cluster.\n");
    return null;
  }

  const { namespace } = await inquirer.prompt([
    {
      type: "list",
      name: "namespace",
      message: "Step 1: Choose a collection to inspect",
      choices: [
        ...overview.collections.map((collection) => ({
          name: `${collection.namespace}${collection.sharded ? ` | ${JSON.stringify(collection.shardKey)}` : ""}`,
          value: collection.namespace
        })),
        { name: "Back", value: "back" }
      ]
    }
  ]);

  if (namespace === "back") {
    return null;
  }

  const [databaseName, collectionName] = namespace.split(".");
  return { databaseName, collectionName, namespace };
}

async function inspectCollectionOnShardFlow(state, overview, target = null) {
  const selectedTarget = target ?? (await promptExistingCollection(overview));
  const collectionTarget = selectedTarget;
  if (!collectionTarget) {
    return;
  }

  const distribution = getCollectionDistribution(
    state,
    collectionTarget.databaseName,
    collectionTarget.collectionName
  );
  printCollectionDistribution(distribution);

  if (!distribution.shardStats.length) {
    console.log("No shard distribution is available for this collection yet.\n");
    return;
  }

  const { shardReplicaSet } = await inquirer.prompt([
    {
      type: "list",
      name: "shardReplicaSet",
      message: "Step 2: Choose which shard you want to inspect directly",
      choices: [
        ...distribution.shardStats.map((entry) => ({
          name: `${entry.shard} | documents: ${entry.count}`,
          value: entry.shard
        })),
        { name: "Back", value: "back" }
      ]
    }
  ]);

  if (shardReplicaSet === "back") {
    return;
  }

  const inspection = inspectShardCollection(
    state,
    collectionTarget.databaseName,
    collectionTarget.collectionName,
    shardReplicaSet
  );
  printShardInspection(inspection);
}

async function runGuidedBooksDemo(state) {
  printBooksDemoIntroduction();

  const overview = getClusterOverview(state);
  const existingDemo = overview.collections.find(
    (collection) => collection.namespace === "library.books"
  );
  let resetCollection = false;
  let currentDocumentCount = 0;

  if (existingDemo) {
    const { demoAction } = await inquirer.prompt([
      {
        type: "list",
        name: "demoAction",
        message: "Step 0: A guided demo collection already exists at library.books. What should happen next?",
        choices: [
          { name: "Reset and run the demo again", value: "reset" },
          { name: "Reuse existing collection and add more data", value: "append" },
          { name: "Back", value: "back" }
        ],
        default: "reset"
      }
    ]);

    if (demoAction === "back") {
      return;
    }

    resetCollection = demoAction === "reset";
    if (!resetCollection) {
      currentDocumentCount = getCollectionDocumentCount(state, "library", "books");
    }
  }

  let shardStrategy;
  let insertCount;
  while (true) {
    const answers = await inquirer.prompt([
      {
        type: "list",
        name: "shardStrategy",
        message: "Step 1: Choose a shard key strategy for library.books",
        choices: [
          { name: "year (range, easy to understand, but low cardinality)", value: { field: "year", mode: "range" } },
          { name: "author (range, better than year here, but still limited)", value: { field: "author", mode: "range" } },
          { name: "_id (range, better baseline)", value: { field: "_id", mode: "range" } },
          { name: "_id (hashed, best for stronger distribution in this demo)", value: { field: "_id", mode: "hashed" } },
          { name: "Back", value: "back" }
        ],
        default: { field: "year", mode: "range" }
      },
      {
        type: "list",
        name: "insertCountChoice",
        message: "Step 2: Choose how much demo data should be inserted",
        choices: [
          { name: "10 documents (quick demo)", value: 10 },
          { name: "100 documents (better distribution view)", value: 100 },
          { name: "500 documents (stronger distribution demo)", value: 500 },
          { name: "1,000,000 documents (large stress demo)", value: 1000000 },
          { name: "Custom", value: "custom" },
          { name: "Back", value: "back" }
        ],
        default: 100
      }
    ]);

    if (answers.shardStrategy === "back" || answers.insertCountChoice === "back") {
      return;
    }

    shardStrategy = answers.shardStrategy;
    if (answers.insertCountChoice !== "custom") {
      insertCount = answers.insertCountChoice;
      break;
    }

    const answer = await inquirer.prompt([
      {
        type: "input",
        name: "insertCount",
        message: "Enter how many books should be inserted:",
        default: 1000,
        validate: (value) =>
          value.trim().toLowerCase() === "back" ||
          (Number.isInteger(Number(value)) && Number(value) >= 1)
            ? true
            : "Insert count must be an integer greater than 0."
      }
    ]);
    if (answer.insertCount.trim().toLowerCase() === "back") {
      return;
    }
    insertCount = Number(answer.insertCount);
    break;
  }

  if (existingDemo && !resetCollection) {
    const requestedShardKey = formatShardKey(shardStrategy.field, shardStrategy.mode);
    const currentShardKey = JSON.stringify(existingDemo.shardKey);

    if (currentShardKey !== requestedShardKey) {
      console.log("\nShard key change detected");
      console.log(`Current shard key: ${currentShardKey}`);
      console.log(`Requested shard key: ${requestedShardKey}`);
      console.log("Action: reshardCollection will run before inserting more data.\n");
    } else {
      console.log("\nThe requested shard key matches the current shard key. Existing sharding will be reused.\n");
    }
  }

  printInsertPlan("library.books", insertCount);
  const result = shardCollection(state, {
    databaseName: "library",
    collectionName: "books",
    shardKeyField: shardStrategy.field,
    shardKeyMode: shardStrategy.mode,
    documents: [],
    resetCollection,
    skipInsert: true
  });
  const inserted = insertDocumentsInBatches(state, {
    databaseName: "library",
    collectionName: "books",
    shardKeyField: shardStrategy.field,
    insertCount,
    startIndex: currentDocumentCount,
    seedMode: "books-demo"
  });
  const distribution = getCollectionDistribution(state, "library", "books");

  console.log("\nGuided demo completed\n");
  console.log(`Namespace: ${result.namespace}`);
  console.log(`Previous shard key: ${JSON.stringify(result.previousShardKey)}`);
  console.log(`Shard key: ${JSON.stringify(result.shardKey)}`);
  console.log(`Action: ${result.actionTaken}`);
  console.log(`Documents inserted: ${inserted}`);
  console.log("Insert completed.");
  console.log(`Shard result: ${JSON.stringify(result.shardResult)}\n`);
  printCollectionDistribution(distribution);
  console.log("Tip: use 'Inspect collection distribution' to see which shard has data and inspect one shard directly.\n");
}

async function runCustomCollectionFlow(state, overview, mode = "custom") {
  const target = await promptCollectionTarget(overview);
  if (!target) {
    return;
  }

  printDocsLink("Shard keys", DOCS.shardKey);
  printDocsLink("Hashed sharding", DOCS.hashedSharding);
  console.log();

  let currentDocumentCount = 0;
  let resetCollection = false;

  if (target.alreadySharded) {
    console.log(
      `\n${target.databaseName}.${target.collectionName} is already sharded with ${JSON.stringify(target.currentShardKey)}.\n`
    );

    const { existingCollectionAction } = await inquirer.prompt([
      {
        type: "list",
        name: "existingCollectionAction",
        message: "Choose how to work with the existing collection",
        choices: [
          { name: "Append more documents", value: "append" },
          { name: "Reset the collection and recreate it", value: "reset" },
          { name: "Only change the shard key if needed", value: "reshard" },
          { name: "Back", value: "back" }
        ],
        default: "append"
      }
    ]);

    if (existingCollectionAction === "back") {
      return;
    }

    resetCollection = existingCollectionAction === "reset";
    if (!resetCollection) {
      currentDocumentCount = getCollectionDocumentCount(
        state,
        target.databaseName,
        target.collectionName
      );
    }
  }

  let answers;
  let insertCount;
  while (true) {
    answers = await inquirer.prompt([
      {
        type: "list",
        name: "continue",
        message: "Continue to shard key and sample insert options?",
        choices: [
          { name: "Continue", value: "continue" },
          { name: "Back", value: "back" }
        ],
        default: "continue"
      },
      {
        type: "input",
        name: "shardKeyField",
        message: "Shard key field:",
        default: "_id",
        when: (answers) => answers.continue === "continue",
        validate: (value) =>
          value.trim().toLowerCase() === "back" || value.trim() ? true : "Shard key field cannot be empty."
      },
      {
        type: "list",
        name: "shardKeyMode",
        message: "Shard key strategy:",
        choices: [
          { name: "Range ({ field: 1 })", value: "range" },
          { name: "Hashed ({ field: \"hashed\" })", value: "hashed" },
          { name: "Back", value: "back" }
        ],
        default: "range",
        when: (answers) => answers.continue === "continue"
      },
      {
        type: "list",
        name: "insertCountChoice",
        message: "How many demo documents should be inserted?",
        choices: [
          { name: "0 documents", value: 0 },
          { name: "20 documents", value: 20 },
          { name: "100 documents", value: 100 },
          { name: "1000 documents", value: 1000 },
          { name: "Custom", value: "custom" },
          { name: "Back", value: "back" }
        ],
        default: mode === "demo" ? 20 : 0,
        when: (answers) => answers.continue === "continue",
      }
    ]);

    if (
      answers.continue === "back" ||
      answers.shardKeyField?.trim?.().toLowerCase() === "back" ||
      answers.shardKeyMode === "back" ||
      answers.insertCountChoice === "back"
    ) {
      return;
    }

    if (answers.insertCountChoice !== "custom") {
      insertCount = answers.insertCountChoice;
      break;
    }

    const customAnswer = await inquirer.prompt([
      {
        type: "input",
        name: "customInsertCount",
        message: "Enter how many demo documents should be inserted:",
        default: 5000,
        validate: (value) =>
          value.trim().toLowerCase() === "back" ||
          (Number.isInteger(Number(value)) && Number(value) >= 0)
            ? true
            : "Insert count must be an integer greater than or equal to 0."
      }
    ]);

    if (customAnswer.customInsertCount.trim().toLowerCase() === "back") {
      return;
    }

    insertCount = Number(customAnswer.customInsertCount);
    break;
  }

  if (target.alreadySharded) {
    const requestedShardKey = formatShardKey(
      answers.shardKeyField.trim(),
      answers.shardKeyMode
    );
    const currentShardKey = JSON.stringify(target.currentShardKey);

    if (currentShardKey !== requestedShardKey) {
      console.log("\nShard key change detected");
      console.log(`Current shard key: ${currentShardKey}`);
      console.log(`Requested shard key: ${requestedShardKey}`);
      console.log("Action: reshardCollection will run before inserting more data.\n");
    } else {
      console.log("\nThe requested shard key matches the current shard key. Existing sharding will be reused.\n");
    }
  }

  printInsertPlan(`${target.databaseName}.${target.collectionName}`, insertCount);
  const result = shardCollection(state, {
    databaseName: target.databaseName,
    collectionName: target.collectionName,
    shardKeyField: answers.shardKeyField.trim(),
    shardKeyMode: answers.shardKeyMode,
    documents: [],
    resetCollection,
    skipInsert: true
  });
  const inserted = insertDocumentsInBatches(state, {
    databaseName: target.databaseName,
    collectionName: target.collectionName,
    shardKeyField: answers.shardKeyField.trim(),
    insertCount,
    startIndex: currentDocumentCount,
    seedMode: "sample-generated"
  });

  console.log("\nCollection updated\n");
  console.log(`Namespace: ${result.namespace}`);
  console.log(`Previous shard key: ${JSON.stringify(result.previousShardKey)}`);
  console.log(`Shard key: ${JSON.stringify(result.shardKey)}`);
  console.log(`Action: ${result.actionTaken}`);
  console.log(`Documents inserted: ${inserted}`);
  console.log("Insert completed.");
  console.log(`Shard result: ${JSON.stringify(result.shardResult)}\n`);
}

async function interactiveShardingMenu() {
  const state = await loadState();
  if (!state) {
    console.log("\nNo cluster has been configured yet.\n");
    return;
  }

  ensureMongosRunning(state);

  let exitMenu = false;
  while (!exitMenu) {
    const overview = getClusterOverview(state);
    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: "Step 3: Learn and experiment with sharded collections",
        choices: [
          { name: "1. List databases and collections (see what already exists)", value: "list" },
          { name: "2. Guided demo: library.books (create sample data and shard it)", value: "guided-books" },
          { name: "3. Custom sharded collection (define your own database and shard key)", value: "custom" },
          { name: "4. Inspect collection distribution (which shard has data?)", value: "inspect-shard" },
          { name: "5. Back", value: "back" }
        ],
        default: "list"
      }
    ]);

    if (action === "list") {
      printCollectionOverview(overview);
      continue;
    }

    if (action === "guided-books") {
      await runGuidedBooksDemo(state);
      continue;
    }

    if (action === "custom") {
      await runCustomCollectionFlow(state, overview);
      continue;
    }

    if (action === "inspect-shard") {
      const target = await promptExistingCollection(overview);
      if (!target) {
        continue;
      }

      const shardPresence = getShardDataPresence(
        state,
        target.databaseName,
        target.collectionName
      );
      printShardDataPresence(target.databaseName, target.collectionName, shardPresence);
      await inspectCollectionOnShardFlow(state, overview, target);
      continue;
    }

    exitMenu = true;
  }
}

async function bringUpCluster(state) {
  const totalSteps = 5;
  const configServices = state.topology.configServers.map((member) => member.serviceName);
  const shardServices = state.topology.shards.flatMap((shard) =>
    shard.members.map((member) => member.serviceName)
  );

  console.log(
    "\nStarting cluster setup. If a step takes too long, you can interrupt with Ctrl+C and run 'up' again. The process is designed to retry safely.\n"
  );

  printStep(
    1,
    totalSteps,
    "Prepare local directories",
    "Creating the folders used by config servers, shard members, and logs."
  );
  await ensureDirectories(state.topology, state.config.storagePath);

  printStep(
    2,
    totalSteps,
    "Start MongoDB containers",
    "Starting config server members and shard replica set members with Docker."
  );
  runCompose(state, ["up", "-d", ...configServices, ...shardServices]);

  printStep(
    3,
    totalSteps,
    "Initialize config server replica set",
    "Waiting for config server members and creating the replica set that stores cluster metadata."
  );
  console.log(`Initializing config replica set '${state.config.configServerReplicaSet}'...`);
  waitForServices(
    state,
    state.topology.configServers.map((member) => member.serviceName)
  );
  runMongoScript(
    state,
    state.topology.configServers[0].serviceName,
    buildReplicaInitScript(
      buildReplicaSetConfig(state.config.configServerReplicaSet, state.topology.configServers, {
        configsvr: true
      })
    )
  );

  printStep(
    4,
    totalSteps,
    "Initialize shard replica sets",
    "Waiting for shard members, then electing a primary in each shard replica set."
  );
  for (const shard of state.topology.shards) {
    console.log(`Initializing shard replica set '${shard.replicaSet}'...`);
    waitForServices(
      state,
      shard.members.map((member) => member.serviceName)
    );
    runMongoScript(
      state,
      shard.members[0].serviceName,
      buildReplicaInitScript(buildReplicaSetConfig(shard.replicaSet, shard.members))
    );
  }

  printStep(
    5,
    totalSteps,
    "Start mongos and register shards",
    "Starting the router, then attaching each shard replica set to the cluster."
  );
  console.log(`Starting mongos on port ${state.config.mongosPort}...`);
  runCompose(state, ["up", "-d", state.topology.mongos.serviceName]);
  waitForMongo(state, state.topology.mongos.serviceName);
  runMongoScript(state, state.topology.mongos.serviceName, buildAddShardsScript(state.topology));
}

function sanitizeProjectName(value) {
  return value.replace(/[^a-zA-Z0-9_-]/g, "-");
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

async function promptConfigReuse(existingState) {
  const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: `Cluster config already exists at ${existingState.config.storagePath}\nChoose how you want to continue`,
      choices: [
        { name: "Reuse existing config", value: "reuse" },
        { name: "Create a new config", value: "replace" },
        { name: "Cancel", value: "cancel" }
      ],
      default: "reuse"
    }
  ]);

  return action;
}

async function createStateFromConfig(config, options = {}) {
  const confirmed = options.confirm === false
    ? true
    : await confirmAction(
      [
        "Create cluster with this topology?",
        `Shards: ${config.shardCount}`,
        `Members per shard: ${config.replicaSetMembers}`,
        `MongoDB version: ${config.mongodbVersion}`,
        `mongos port: ${config.mongosPort}`,
        `Storage path: ${config.storagePath}`
      ].join("\n"),
      true
    );

  if (!confirmed) {
    return null;
  }

  const topology = buildTopology(config);
  const files = await writeClusterFiles(config, topology);
  const state = {
    projectName: sanitizeProjectName(DEFAULT_PROJECT_NAME),
    config,
    topology,
    ...files
  };

  await saveState(state);
  return state;
}

async function resolveStateForUp(options = {}) {
  const explicitOptions = hasExplicitUpOptions(options);
  let state = await loadState();

  if (state && !explicitOptions && !options.quickstart) {
    const action = await promptConfigReuse(state);

    if (action === "cancel") {
      return null;
    }

    if (action === "reuse") {
      console.log(`\nUsing existing cluster config from ${state.config.storagePath}\n`);
      return state;
    }

    await fs.rm(state.config.storagePath, { recursive: true, force: true });
    state = null;
  }

  const desiredConfig = options.quickstart
    ? buildQuickstartConfig(options)
    : await resolveUpConfig(options);

  if (!state) {
    return createStateFromConfig(desiredConfig, { confirm: !explicitOptions && !options.quickstart });
  }

  if (configsMatch(state.config, desiredConfig)) {
    console.log(`\nUsing existing cluster config from ${state.config.storagePath}\n`);
    return state;
  }

  if (options.force) {
    await fs.rm(state.config.storagePath, { recursive: true, force: true });
    return createStateFromConfig(desiredConfig, { confirm: false });
  }

  throw new Error(
    [
      `A different cluster config already exists at ${state.config.storagePath}.`,
      "Run 'mongodb-cli-lab clean' first or rerun the command with '--force'."
    ].join(" ")
  );
}

async function requireState() {
  const state = await loadState();

  if (!state) {
    throw new Error("No cluster state found. Run 'mongodb-cli-lab up' first.");
  }

  return state;
}

async function runUp(options = {}) {
  ensureDockerAvailable();
  const state = await resolveStateForUp(options);
  if (!state) {
    return null;
  }

  await bringUpCluster(state);

  console.log("\nCluster ready\n");
  printTopologyDiagram(state);
  console.log("Connection string:");
  console.log(`mongodb://localhost:${state.config.mongosPort}`);
  console.log("\nIf initialization is interrupted, rerunning 'up' will retry safely.\n");
  return state;
}

async function runDown() {
  ensureDockerAvailable();
  const state = await requireState();
  printStep(1, 1, "Stop cluster", "Stopping the running Docker containers for this lab.");
  runCompose(state, ["down"]);
}

async function runStatus() {
  ensureDockerAvailable();

  const state = await loadActiveClusterState();
  if (!state) {
    console.log("\nNo running MongoDB CLI Lab cluster was found for this project.\n");

    if (await confirmAction("Do you want to create a new cluster now?", true)) {
      await runUp();
    }

    return;
  }

  const containers = getComposeContainers(state);
  printClusterSummary(state, containers);
  printTopologyDiagram(state);
  printReplicaSetHealth(state, containers);
  printTopologyDetails(state, containers);
  printContainersTable(containers);
}

async function runClean() {
  ensureDockerAvailable();
  const state = await requireState();

  printStep(1, 2, "Stop containers", "Stopping and removing containers, networks, and volumes for this lab.");
  try {
    runCompose(state, ["down", "--remove-orphans", "-v"]);
  } catch {
    // Keep removing files even if Docker cleanup fails.
  }

  printStep(2, 2, "Delete generated files", "Removing generated configuration, state, and local data directories.");
  await deleteState(state.config.storagePath);
  await fs.rm(state.config.storagePath, { recursive: true, force: true });
}

async function showNodeDetails() {
  ensureDockerAvailable();

  const state = await loadActiveClusterState();
  if (!state) {
    console.log("\nNo running MongoDB CLI Lab cluster was found for this project.\n");
    return;
  }

  const containers = getComposeContainers(state);
  printClusterSummary(state, containers);
  printTopologyDiagram(state);
  printReplicaSetHealth(state, containers);
  printTopologyDetails(state, containers);
  printContainersTable(containers);
}

async function interactiveClusterMenu() {
  const state = await loadState();
  if (!state) {
    console.log("\nNo cluster has been configured yet.\n");
    return;
  }

  let exitMenu = false;

  while (!exitMenu) {
    const containers = getComposeContainers(state);
    const runningCount = containers.filter((container) => container.State === "running").length;

    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: `Step 4: Manage the cluster lifecycle\n${runningCount}/${countExpectedNodes(state)} nodes are currently running`,
        choices: [
          { name: "1. Show summary and node list (see the current topology)", value: "show" },
          { name: "2. Start cluster (bring containers up)", value: "up" },
          { name: "3. Stop cluster (bring containers down)", value: "down" },
          { name: "4. Delete cluster and files (remove local lab data)", value: "clean" },
          { name: "5. Back", value: "back" }
        ],
        default: "show"
      }
    ]);

    if (action === "show") {
      await showNodeDetails();
      continue;
    }

    if (action === "up") {
      await runUp();
      continue;
    }

    if (action === "down") {
      if (await confirmAction("Stop all cluster containers?")) {
        await runDown();
      }
      continue;
    }

    if (action === "clean") {
      if (await confirmAction("Delete containers, volumes and generated files?")) {
        await runClean();
        exitMenu = true;
      }
      continue;
    }

    exitMenu = true;
  }
}

async function interactiveMainMenu() {
  ensureDockerAvailable();

  let exitMenu = false;

  while (!exitMenu) {
    const activeState = await loadActiveClusterState();
    const savedState = activeState ?? await loadState();
    const stateLabel = activeState
      ? `Active cluster: ${activeState.config.shardCount} shard(s), ${activeState.config.replicaSetMembers} member(s) each`
      : savedState
        ? "No cluster running right now"
        : "No cluster configured yet";

    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: `MongoDB CLI Lab\n${stateLabel}\nChoose the next step in the lab`,
        choices: [
          { name: "1. Create or start cluster (infrastructure setup)", value: "up" },
          { name: "2. Show cluster status and nodes (understand the topology)", value: "status" },
          { name: "3. Collections and sharding (learn with data)", value: "collections" },
          { name: "4. Manage cluster (stop, restart, or delete)", value: "manage" },
          { name: "5. Exit", value: "exit" }
        ],
        default: "up"
      }
    ]);

    if (action === "up") {
      await runUp();
      continue;
    }

    if (action === "status") {
      try {
        await runStatus();
      } catch (error) {
        console.log(`\n${error.message}\n`);
      }
      continue;
    }

    if (action === "collections") {
      try {
        await interactiveShardingMenu();
      } catch (error) {
        console.log(`\n${error.message}\n`);
      }
      continue;
    }

    if (action === "manage") {
      try {
        await interactiveClusterMenu();
      } catch (error) {
        console.log(`\n${error.message}\n`);
      }
      continue;
    }

    exitMenu = true;
  }
}

async function runQuickstart(options = {}) {
  const config = buildQuickstartConfig(options);
  printQuickstartPlan(config);
  const confirmed = await confirmAction("Proceed with quickstart setup and demo?", true);
  if (!confirmed) {
    console.log("\nQuickstart cancelled.\n");
    return;
  }

  const state = await runUp({ ...options, quickstart: true });
  if (!state) {
    return;
  }

  await runQuickstartDemo(state);
}

export {
  interactiveMainMenu,
  runClean,
  runDown,
  runQuickstart,
  runStatus,
  runUp
};
