#!/usr/bin/env node

import { existsSync, rmSync, statSync } from "node:fs";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { execFileSync } from "node:child_process";
import inquirer from "inquirer";
import {
  buildQuickstartConfig,
  configsMatch,
  getAvailableSampleDatabases,
  hasExplicitUpOptions,
  resolveUpConfig
} from "./lib/config.js";

const DEFAULT_STORAGE_PATH = "./mongodb-cli-lab";
const DEFAULT_PROJECT_NAME = "mongodb-cli-lab";
const COMPOSE_PROJECT_NAME = sanitizeProjectName(DEFAULT_PROJECT_NAME);
const INTERNAL_MONGO_PORT = 27017;
const SEARCH_STATE_MARKER = "__MONGODB_CLI_LAB_SEARCH_JSON__";
const SEARCH_SAMPLE_DATA_URL = "https://atlas-education.s3.amazonaws.com/sampledata.archive";
const SEARCH_MONGOD_IMAGE = "mongodb/mongodb-community-server:latest";
const SEARCH_MONGOT_IMAGE = "mongodb/mongodb-community-search:latest";
const SEARCH_SERVICE_NAME = "search-mongod";
const MONGOT_SERVICE_NAME = "search-mongot";
const SEARCH_REPLICA_SET = "searchRs";
const SEARCH_PASSWORD = "mongotPassword";
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
  if (config.topology === "standalone") {
    return {
      configServers: [],
      shards: [],
      replicaSet: null,
      standalone: {
        serviceName: "mongo-1",
        hostPort: config.mongosPort,
        containerPort: INTERNAL_MONGO_PORT,
        dbPath: path.join(config.storagePath, "standalone", "mongo-1"),
        replicaSet: config.features?.search ? "rs0" : null
      },
      mongos: null,
      queryRouter: {
        serviceName: "mongo-1",
        hostPort: config.mongosPort,
        containerPort: INTERNAL_MONGO_PORT,
        sampleArchiveFile: path.join(config.storagePath, "sampledata.archive")
      },
      search: config.features?.search
        ? {
            mongodServiceName: "mongo-1",
            mongotServiceName: MONGOT_SERVICE_NAME,
            replicaSet: "rs0",
            mongodPort: config.mongosPort,
            mongotPort: config.search.mongotPort,
            metricsPort: config.search.metricsPort,
            usesPrimaryNode: true,
            seedServiceName: "mongo-1",
            mongotDataPath: path.join(config.storagePath, "search", "mongot"),
            mongotConfigFile: path.join(config.storagePath, "search", "mongot.conf"),
            passwordFile: path.join(config.storagePath, "search", "pwfile"),
            sampleArchiveFile: path.join(config.storagePath, "search", "sampledata.archive")
          }
        : null
    };
  }

  if (config.topology === "replica-set") {
    const members = Array.from({ length: config.replicaSetMembers }, (_, index) => ({
      id: `rs0-${index + 1}`,
      serviceName: `rs0-${index + 1}`,
      replicaSet: "rs0",
      dbPath: path.join(config.storagePath, "replica-set", `member${index + 1}`),
      hostPort: config.mongosPort + index,
      advertisedHostname: `rs0-${index + 1}.localhost`,
      advertisedHost: config.features?.search
        ? `rs0-${index + 1}:${INTERNAL_MONGO_PORT}`
        : `rs0-${index + 1}.localhost:${config.mongosPort + index}`,
      externalHost: `rs0-${index + 1}.localhost:${config.mongosPort + index}`
    }));

    return {
      configServers: [],
      shards: [],
      replicaSet: {
        name: "rs0",
        members
      },
      standalone: null,
      mongos: null,
      queryRouter: {
        serviceName: members[0].serviceName,
        hostPort: config.mongosPort,
        containerPort: INTERNAL_MONGO_PORT,
        sampleArchiveFile: path.join(config.storagePath, "sampledata.archive")
      },
      search: config.features?.search
        ? {
            mongodServiceName: members[0].serviceName,
            mongotServiceName: MONGOT_SERVICE_NAME,
            replicaSet: "rs0",
            mongodPort: config.mongosPort,
            mongotPort: config.search.mongotPort,
            metricsPort: config.search.metricsPort,
            usesPrimaryNode: true,
            seedServiceName: members[0].serviceName,
            mongotDataPath: path.join(config.storagePath, "search", "mongot"),
            mongotConfigFile: path.join(config.storagePath, "search", "mongot.conf"),
            passwordFile: path.join(config.storagePath, "search", "pwfile"),
            sampleArchiveFile: path.join(config.storagePath, "search", "sampledata.archive")
          }
        : null
    };
  }

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
    replicaSet: null,
    standalone: null,
    mongos: {
      serviceName: "mongos",
      hostPort: config.mongosPort,
      containerPort: INTERNAL_MONGO_PORT,
      sampleArchiveFile: path.join(config.storagePath, "sampledata.archive")
    },
    queryRouter: {
      serviceName: "mongos",
      hostPort: config.mongosPort,
      containerPort: INTERNAL_MONGO_PORT,
      sampleArchiveFile: path.join(config.storagePath, "sampledata.archive")
    },
    search: config.features?.search
      ? {
          mongodServiceName: SEARCH_SERVICE_NAME,
          mongotServiceName: MONGOT_SERVICE_NAME,
          replicaSet: SEARCH_REPLICA_SET,
          mongodPort: config.search.mongodPort,
          mongotPort: config.search.mongotPort,
          metricsPort: config.search.metricsPort,
          usesPrimaryNode: false,
          seedServiceName: SEARCH_SERVICE_NAME,
          dbPath: path.join(config.storagePath, "search", "mongod"),
          mongotDataPath: path.join(config.storagePath, "search", "mongot"),
          mongodConfigFile: path.join(config.storagePath, "search", "mongod.conf"),
          mongotConfigFile: path.join(config.storagePath, "search", "mongot.conf"),
          passwordFile: path.join(config.storagePath, "search", "pwfile"),
          sampleArchiveFile: path.join(config.storagePath, "search", "sampledata.archive")
        }
      : null
    
  };
}

function createSearchMongodConfig() {
  return `storage:
  dbPath: /data/db
net:
  port: 27017
  bindIp: 0.0.0.0
setParameter:
  searchIndexManagementHostAndPort: ${MONGOT_SERVICE_NAME}:27028
  mongotHost: ${MONGOT_SERVICE_NAME}:27028
  skipAuthenticationToSearchIndexManagementServer: false
  useGrpcForSearch: true
replication:
  replSetName: ${SEARCH_REPLICA_SET}
`;
}

function createSearchMongotConfig() {
  return `syncSource:
  replicaSet:
    hostAndPort: "${SEARCH_SERVICE_NAME}:27017"
    username: mongotUser
    passwordFile: /mongot-community/pwfile
    authSource: admin
    tls: false
    readPreference: primaryPreferred
storage:
  dataPath: "data/mongot"
server:
  grpc:
    address: "${MONGOT_SERVICE_NAME}:27028"
    tls:
      mode: "disabled"
metrics:
  enabled: true
  address: "${MONGOT_SERVICE_NAME}:9946"
healthCheck:
  address: "${MONGOT_SERVICE_NAME}:8080"
logging:
  verbosity: INFO
`;
}

function buildSearchSetParameterArgs(topology) {
  if (!topology.search) {
    return [];
  }

  return [
    "--setParameter",
    `searchIndexManagementHostAndPort=${topology.search.mongotServiceName}:27028`,
    "--setParameter",
    `mongotHost=${topology.search.mongotServiceName}:27028`,
    "--setParameter",
    "skipAuthenticationToSearchIndexManagementServer=false",
    "--setParameter",
    "useGrpcForSearch=true"
  ];
}

function createMongotConfig(topology) {
  return `syncSource:
  replicaSet:
    hostAndPort: "${topology.search.seedServiceName}:27017"
    username: mongotUser
    passwordFile: /mongot-community/pwfile
    authSource: admin
    tls: false
    readPreference: primaryPreferred
storage:
  dataPath: "data/mongot"
server:
  grpc:
    address: "${topology.search.mongotServiceName}:27028"
    tls:
      mode: "disabled"
metrics:
  enabled: true
  address: "${topology.search.mongotServiceName}:9946"
healthCheck:
  address: "${topology.search.mongotServiceName}:8080"
logging:
  verbosity: INFO
`;
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
    indent(2, `image: ${service.image ?? `mongo:${service.imageTag}`}`),
    indent(2, `container_name: ${service.containerName ?? service.name}`)
  ];

  if (service.dependsOn?.length) {
    lines.push(indent(2, "depends_on:"));
    for (const dependency of service.dependsOn) {
      lines.push(indent(3, `- ${dependency}`));
    }
  }

  if (service.extraHosts?.length) {
    lines.push(indent(2, "extra_hosts:"));
    for (const host of service.extraHosts) {
      lines.push(indent(3, `- ${yamlQuote(host)}`));
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
  if (config.topology === "standalone") {
    const services = [
      {
        name: topology.standalone.serviceName,
        containerName: topology.standalone.serviceName,
        imageTag: config.mongodbVersion,
        command: [
          "mongod",
          ...(topology.standalone.replicaSet ? ["--replSet", topology.standalone.replicaSet] : []),
          "--bind_ip_all",
          "--port",
          INTERNAL_MONGO_PORT,
          "--dbpath",
          "/data/db",
          ...buildSearchSetParameterArgs(topology)
        ],
        ports: [`${topology.standalone.hostPort}:${topology.standalone.containerPort}`],
        volumes: [
          `${topology.standalone.dbPath}:/data/db`,
          `${topology.queryRouter.sampleArchiveFile}:/sampledata.archive`
        ]
      }
    ];

    if (topology.search) {
      services.push(
        {
          name: topology.search.mongotServiceName,
          containerName: topology.search.mongotServiceName,
          image: SEARCH_MONGOT_IMAGE,
          dependsOn: [topology.standalone.serviceName],
          command: ["mongot", "--config", "/mongot-community/config.default.yml"],
          ports: [`${topology.search.mongotPort}:27028`, `${topology.search.metricsPort}:9946`],
          volumes: [
            `${topology.search.mongotDataPath}:/data/mongot`,
            `${topology.search.mongotConfigFile}:/mongot-community/config.default.yml:ro`,
            `${topology.search.passwordFile}:/mongot-community/pwfile:ro`
          ]
        }
      );
    }

    return ["services:", ...services.map(serviceToYaml)].join("\n");
  }

  if (config.topology === "replica-set") {
    const replicaSetExtraHosts = topology.replicaSet.members.map(
      (member) => `${member.advertisedHostname}:host-gateway`
    );

    const services = topology.replicaSet.members.map((member, index) => ({
      name: member.serviceName,
      containerName: member.serviceName,
      imageTag: config.mongodbVersion,
      command: [
        "mongod",
        "--replSet",
        topology.replicaSet.name,
        "--bind_ip_all",
        "--port",
        INTERNAL_MONGO_PORT,
        "--dbpath",
        "/data/db",
        ...buildSearchSetParameterArgs(topology)
      ],
      ports: [`${member.hostPort}:${INTERNAL_MONGO_PORT}`],
      extraHosts: replicaSetExtraHosts,
      volumes: [
        `${member.dbPath}:/data/db`,
        `${topology.queryRouter.sampleArchiveFile}:/sampledata.archive`
      ]
    }));

    if (topology.search) {
      services.push(
        {
          name: topology.search.mongotServiceName,
          containerName: topology.search.mongotServiceName,
          image: SEARCH_MONGOT_IMAGE,
          dependsOn: [topology.search.seedServiceName],
          command: ["mongot", "--config", "/mongot-community/config.default.yml"],
          ports: [`${topology.search.mongotPort}:27028`, `${topology.search.metricsPort}:9946`],
          volumes: [
            `${topology.search.mongotDataPath}:/data/mongot`,
            `${topology.search.mongotConfigFile}:/mongot-community/config.default.yml:ro`,
            `${topology.search.passwordFile}:/mongot-community/pwfile:ro`
          ]
        }
      );
    }

    return ["services:", ...services.map(serviceToYaml)].join("\n");
  }

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
      volumes: [
        path.join(config.storagePath, "logs") + ":/var/log/mongodb",
        `${topology.mongos.sampleArchiveFile}:/sampledata.archive`
      ]
    }
  ];

  if (topology.search) {
    services.push(
      {
        name: topology.search.mongodServiceName,
        containerName: topology.search.mongodServiceName,
        image: SEARCH_MONGOD_IMAGE,
        command: [
          "mongod",
          "--config",
          "/etc/mongod.conf",
          "--replSet",
          topology.search.replicaSet
        ],
        ports: [`${topology.search.mongodPort}:${INTERNAL_MONGO_PORT}`],
        volumes: [
          `${topology.search.dbPath}:/data/db`,
          `${topology.search.mongodConfigFile}:/etc/mongod.conf:ro`,
          `${topology.search.sampleArchiveFile}:/sampledata.archive`
        ]
      },
      {
        name: topology.search.mongotServiceName,
        containerName: topology.search.mongotServiceName,
        image: SEARCH_MONGOT_IMAGE,
        dependsOn: [topology.search.mongodServiceName],
        command: [
          "mongot",
          "--config",
          "/mongot-community/config.default.yml"
        ],
        ports: [
          `${topology.search.mongotPort}:27028`,
          `${topology.search.metricsPort}:9946`
        ],
        volumes: [
          `${topology.search.mongotDataPath}:/data/mongot`,
          `${topology.search.mongotConfigFile}:/mongot-community/config.default.yml:ro`,
          `${topology.search.passwordFile}:/mongot-community/pwfile:ro`
        ]
      }
    );
  }

  return ["services:", ...services.map(serviceToYaml)].join("\n");
}

function buildReplicaSetConfig(replicaSet, members, options = {}) {
  return {
    _id: replicaSet,
    ...(options.configsvr ? { configsvr: true } : {}),
    members: members.map((member, index) => ({
      _id: index,
      host: member.advertisedHost ?? `${member.serviceName}:${INTERNAL_MONGO_PORT}`
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
      const primaryMember = (status.members || []).find((member) => member.stateStr === "PRIMARY");
      if (primaryMember) {
        print("Primary elected for " + cfg._id + ": " + primaryMember.name);
        quit(0);
      }

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

  if (topology.standalone) {
    directories.push(path.join(storagePath, "standalone"), topology.standalone.dbPath);
  }

  if (topology.replicaSet) {
    directories.push(
      path.join(storagePath, "replica-set"),
      ...topology.replicaSet.members.map((member) => member.dbPath)
    );
  }

  if (topology.search) {
    directories.push(
      path.join(storagePath, "search"),
      topology.search.mongotDataPath
    );
  }

  await Promise.all(directories.map((directory) => fs.mkdir(directory, { recursive: true })));
}

function runCommand(command, args, options = {}) {
  return execFileSync(command, args, {
    stdio: options.capture ? ["ignore", "pipe", "pipe"] : "inherit",
    encoding: options.capture ? "utf8" : undefined
  });
}

function dockerImageExists(imageName) {
  try {
    runCommand("docker", ["image", "inspect", imageName], { capture: true });
    return true;
  } catch {
    return false;
  }
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
  const fileWrites = [
    fs.writeFile(composeFile, generateComposeFile(config, topology), "utf8"),
    fs.writeFile(clusterConfigFile, JSON.stringify(config, null, 2), "utf8"),
    fs.writeFile(topologyFile, JSON.stringify(topology, null, 2), "utf8")
  ];

  if (topology.search) {
    if (existsSync(topology.search.passwordFile)) {
      await fs.chmod(topology.search.passwordFile, 0o600);
    }

    fileWrites.push(
      fs.writeFile(topology.search.mongotConfigFile, createMongotConfig(topology), "utf8"),
      fs.writeFile(topology.search.passwordFile, SEARCH_PASSWORD, { encoding: "utf8", mode: 0o400 })
    );
  }

  await Promise.all(fileWrites);

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

    if (mount.Source.includes(`${path.sep}standalone${path.sep}`)) {
      return path.dirname(path.dirname(mount.Source));
    }

    if (mount.Source.includes(`${path.sep}replica-set${path.sep}`)) {
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

  for (const containerId of containerIds) {
    const inspectOutput = tryRunCommand("docker", ["inspect", containerId], { capture: true });
    if (!inspectOutput) {
      continue;
    }

    let details;
    try {
      details = JSON.parse(inspectOutput);
    } catch {
      continue;
    }

    const containerDetails = Array.isArray(details) ? details[0] : null;
    const storagePath = inferStoragePathFromContainerInspect(containerDetails);
    if (!storagePath) {
      continue;
    }

    const discoveredState = await loadStateFromStoragePath(storagePath);
    if (!discoveredState) {
      continue;
    }

    await saveState(discoveredState);
    return discoveredState;
  }

  return null;
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
  if (state.config.topology === "standalone") {
    return 1 + (state.topology.search ? 1 : 0);
  }

  if (state.config.topology === "replica-set") {
    return state.topology.replicaSet.members.length + (state.topology.search ? 1 : 0);
  }

  return (
    state.topology.configServers.length +
    state.topology.shards.reduce((total, shard) => total + shard.members.length, 0) +
    1 +
    (state.topology.search ? 2 : 0)
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

  if (serviceName === "mongo-1") {
    return "standalone";
  }

  if (serviceName === "mongos") {
    return "mongos";
  }

  if (serviceName === MONGOT_SERVICE_NAME) {
    return "mongot";
  }

  return "shardsvr";
}

function buildDisplayName(serviceName, port = INTERNAL_MONGO_PORT) {
  if (serviceName.startsWith("cfg")) {
    return `cfg-${port}`;
  }

  if (serviceName === "mongo-1") {
    return `mongo-${port}`;
  }

  if (serviceName.startsWith("rs0-")) {
    return `${serviceName}-${port}`;
  }

  if (serviceName === "mongos") {
    return `mongos-${port}`;
  }

  if (serviceName === MONGOT_SERVICE_NAME) {
    return `mongot-${port}`;
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
  console.log(`Topology: ${state.config.topology}`);
  console.log(`MongoDB version: ${state.config.mongodbVersion}`);
  if (state.config.topology === "sharded") {
    console.log(`Shards: ${state.config.shardCount}`);
    console.log(`Replica set members per shard: ${state.config.replicaSetMembers}`);
  } else if (state.config.topology === "replica-set") {
    console.log(`Replica set members: ${state.config.replicaSetMembers}`);
  }
  console.log(`Search support: ${state.config.features?.search ? "enabled" : "disabled"}`);
  console.log(`Nodes running: ${runningCount}/${expectedNodes}`);
  console.log(`Connection: mongodb://localhost:${state.config.mongosPort}\n`);
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

  if (state.config.topology === "standalone") {
    const container = containerMap.get(state.topology.standalone.serviceName);
    console.log(
      `- standalone (${state.topology.standalone.serviceName}) | state: ${containerStateLabel(container)} | port: ${containerPortLabel(container)}`
    );
    console.log();
  } else if (state.config.topology === "replica-set") {
    console.log(`Replica set: ${state.topology.replicaSet.name}`);
    for (const member of state.topology.replicaSet.members) {
      const container = containerMap.get(member.serviceName);
      console.log(
        `- ${buildDisplayName(member.serviceName)} (${member.serviceName}) | state: ${containerStateLabel(container)} | port: ${containerPortLabel(container)} | host: ${member.externalHost ?? member.advertisedHost}`
      );
    }
    console.log();
  } else {

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
  }

  if (state.topology.search) {
    console.log(`\nSearch`);
    const mongotContainer = containerMap.get(state.topology.search.mongotServiceName);
    console.log(
      `- ${buildDisplayName(state.topology.search.mongotServiceName, 27028)} (${state.topology.search.mongotServiceName}) | state: ${containerStateLabel(mongotContainer)} | port: ${containerPortLabel(mongotContainer, 27028)}`
    );
  }

  console.log();
}

function printReplicaSetHealth(state, containers) {
  const containerMap = buildContainerMap(containers);
  console.log("Replica set health\n");

  if (state.config.topology === "standalone") {
    const standaloneState = containerMap.get(state.topology.standalone.serviceName)?.State ?? "not created";
    console.log(`- standalone | state: ${standaloneState}\n`);
  } else if (state.config.topology === "replica-set") {
    const runningMembers = state.topology.replicaSet.members.filter(
      (member) => containerMap.get(member.serviceName)?.State === "running"
    ).length;
    console.log(
      `- ${state.topology.replicaSet.name} | running members: ${runningMembers}/${state.topology.replicaSet.members.length}\n`
    );
  } else {
  const configRunning = state.topology.configServers.filter(
    (member) => containerMap.get(member.serviceName)?.State === "running"
  ).length;

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

  if (state.topology.search) {
    const searchState = containerMap.get(state.topology.search.mongodServiceName)?.State ?? "not created";
    const mongotState = containerMap.get(state.topology.search.mongotServiceName)?.State ?? "not created";
    console.log("Search services\n");
    console.log(
      `- MongoDB Search enabled on ${state.topology.search.mongodServiceName} | state: ${searchState}`
    );
    console.log(`- ${state.topology.search.mongotServiceName} | state: ${mongotState}\n`);
  }
}

function printTopologyDiagram(state) {
  console.log("Cluster structure\n");
  if (state.config.topology === "standalone") {
    console.log("        +---------------------------+");
    console.log("        | standalone                |");
    console.log(`        | localhost:${state.config.mongosPort}${" ".repeat(Math.max(0, 12 - String(state.config.mongosPort).length))}|`);
    console.log("        +---------------------------+");
  } else if (state.config.topology === "replica-set") {
    console.log("        +---------------------------+");
    console.log(`        | ${state.topology.replicaSet.name.padEnd(27, " ")}|`);
    console.log(`        | localhost:${state.config.mongosPort}${" ".repeat(Math.max(0, 12 - String(state.config.mongosPort).length))}|`);
    console.log("        +-------------+-------------+");
    for (const member of state.topology.replicaSet.members) {
      console.log(`               - ${member.serviceName}`);
    }
  } else {
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
  }

  if (state.topology.search) {
    console.log("               +------v------+");
    console.log(`               | search      |`);
    console.log(`               | ${"mongot".padEnd(11, " ")}|`);
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

function getPrimaryConnectionString(state) {
  if (state.config.topology === "standalone" && state.config.features?.search) {
    return getDirectConnectionString(state);
  }

  if (state.config.topology === "replica-set") {
    if (state.config.features?.search) {
      return getDirectConnectionString(state);
    }

    const hosts = state.topology.replicaSet.members
      .map((member) => member.externalHost ?? `${member.advertisedHostname}:${member.hostPort}`)
      .join(",");
    return `mongodb://${hosts}/?replicaSet=${state.topology.replicaSet.name}`;
  }

  return `mongodb://localhost:${state.config.mongosPort}`;
}

function getReplicaSetPrimaryMember(state) {
  if (state.config.topology !== "replica-set") {
    return null;
  }

  try {
    const status = runMongoJson(
      state,
      state.topology.replicaSet.members[0].serviceName,
      `
const status = db.adminCommand({ replSetGetStatus: 1 });
const primaryName = (status.members || []).find((member) => member.stateStr === "PRIMARY")?.name ?? null;
const result = { primaryName };
`.trim()
    );

    if (!status.primaryName) {
      return null;
    }

    return state.topology.replicaSet.members.find((member) =>
      [member.advertisedHost, member.externalHost, `${member.serviceName}:${INTERNAL_MONGO_PORT}`].includes(status.primaryName)
    ) ?? null;
  } catch {
    return null;
  }
}

function getDirectConnectionString(state) {
  if (state.config.topology === "replica-set") {
    const primaryMember = getReplicaSetPrimaryMember(state);
    if (primaryMember) {
      return `mongodb://localhost:${primaryMember.hostPort}/?directConnection=true`;
    }
  }

  return `mongodb://localhost:${state.config.mongosPort}/?directConnection=true`;
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

function runSearchContainerCommand(state, serviceName, args = [], options = {}) {
  return runCompose(state, ["exec", "-T", serviceName, ...args], options);
}

function runContainerCommand(state, serviceName, args = [], options = {}) {
  return runCompose(state, ["exec", "-T", serviceName, ...args], options);
}

function getSearchMongoServiceCandidates(state) {
  if (!state.topology.search) {
    return [];
  }

  if (!state.topology.search.usesPrimaryNode) {
    return [state.topology.search.mongodServiceName];
  }

  if (state.config.topology === "replica-set") {
    return state.topology.replicaSet.members.map((member) => member.serviceName);
  }

  if (state.config.topology === "standalone") {
    return [state.topology.standalone.serviceName];
  }

  return [state.topology.search.mongodServiceName];
}

function getSearchMongoServiceName(state, options = {}) {
  const { writablePrimary = false } = options;
  const candidates = getSearchMongoServiceCandidates(state);

  if (!writablePrimary || candidates.length <= 1) {
    return candidates[0] ?? state.topology.search.mongodServiceName;
  }

  if (state.config.topology === "replica-set") {
    const primaryMember = getReplicaSetPrimaryMember(state);
    if (primaryMember) {
      return primaryMember.serviceName;
    }
  }

  for (const serviceName of candidates) {
    try {
      const result = runMongoJson(
        state,
        serviceName,
        `
const hello = (db.hello && db.hello()) || db.isMaster();
const result = {
  isWritablePrimary: hello.isWritablePrimary === true || hello.ismaster === true
};
`.trim()
      );

      if (result.isWritablePrimary) {
        return serviceName;
      }
    } catch {
      // Try the next candidate.
    }
  }

  return candidates[0] ?? state.topology.search.mongodServiceName;
}

function runSearchMongoJson(state, script) {
  const serviceName = getSearchMongoServiceName(state, { writablePrimary: true });
  const output = runCompose(
    state,
    ["exec", "-T", serviceName, "mongosh", "--quiet", "--eval", `${script}\nprint("${SEARCH_STATE_MARKER}" + JSON.stringify(result));`],
    { capture: true }
  );

  const line = output
    .split("\n")
    .map((entry) => entry.trim())
    .find((entry) => entry.startsWith(SEARCH_STATE_MARKER));

  if (!line) {
    throw new Error("Could not parse MongoDB Search command output.");
  }

  return JSON.parse(line.slice(SEARCH_STATE_MARKER.length));
}

function ensureSearchEnabled(state) {
  if (!state.config.features?.search || !state.topology.search) {
    throw new Error("This cluster was not created with Search support enabled.");
  }
}

function getClusterSampleDatabases(state, requestedDatabases = state.config.sampleDatabases ?? []) {
  const available = new Set(getAvailableSampleDatabases());
  const selection = requestedDatabases.length ? requestedDatabases : state.config.sampleDatabases ?? [];

  for (const databaseName of selection) {
    if (!available.has(databaseName)) {
      throw new Error(`Unknown sample database '${databaseName}'.`);
    }
  }

  return selection;
}

function ensureClusterSampleArchive(state) {
  if (existsSync(state.topology.queryRouter.sampleArchiveFile)) {
    const stat = statSync(state.topology.queryRouter.sampleArchiveFile);
    if (stat.isFile()) {
      return false;
    }

    if (stat.isDirectory()) {
      rmSync(state.topology.queryRouter.sampleArchiveFile, { recursive: true, force: true });
    }
  }

  console.log("Downloading the sample archive used by the cluster lab.");
  runCommand("curl", ["-L", SEARCH_SAMPLE_DATA_URL, "-o", state.topology.queryRouter.sampleArchiveFile]);
  return true;
}

function ensureSampleArchiveMountedOnService(state, serviceName) {
  try {
    runContainerCommand(state, serviceName, [
      "sh",
      "-lc",
      "test -f /sampledata.archive"
    ]);
    return;
  } catch {
    console.log(`Refreshing ${serviceName} so the sample archive is mounted correctly.`);
    runCompose(state, ["up", "-d", "--force-recreate", serviceName]);
    waitForMongo(state, serviceName);
  }
}

function getClusterSampleDatabaseCounts(state, databaseNames) {
  const serviceName = state.config.topology === "replica-set"
    ? getSearchMongoServiceName(state, { writablePrimary: true })
    : state.topology.queryRouter.serviceName;

  return databaseNames.map((databaseName) => ({
    databaseName,
    count: runMongoJson(
      state,
      serviceName,
      `
const database = db.getSiblingDB(${JSON.stringify(databaseName)});
const result = {
  count: database.getCollectionNames().reduce((total, collectionName) => {
    return total + database.getCollection(collectionName).countDocuments({});
  }, 0)
};
`.trim()
    ).count
  }));
}

function ensureClusterSampleDatabasesImported(state, requestedDatabases) {
  const selectedDatabases = getClusterSampleDatabases(state, requestedDatabases);
  if (!selectedDatabases.length) {
    return [];
  }

  ensureClusterSampleArchive(state);

  if (state.config.topology === "replica-set") {
    for (const member of state.topology.replicaSet.members) {
      ensureSampleArchiveMountedOnService(state, member.serviceName);
    }
  } else {
    ensureSampleArchiveMountedOnService(state, state.topology.queryRouter.serviceName);
  }

  const restoreServiceName = state.config.topology === "replica-set"
    ? getSearchMongoServiceName(state, { writablePrimary: true })
    : state.topology.queryRouter.serviceName;

  try {
    const counts = getClusterSampleDatabaseCounts(state, selectedDatabases);
    if (counts.every((database) => database.count > 0)) {
      return counts;
    }
  } catch {
    // Continue to restore.
  }

  runContainerCommand(state, restoreServiceName, [
    "mongorestore",
    "--host",
    "localhost",
    "--port",
    String(INTERNAL_MONGO_PORT),
    "--archive=/sampledata.archive",
    ...selectedDatabases.map((databaseName) => `--nsInclude=${databaseName}.*`)
  ]);

  for (let attempt = 0; attempt < 180; attempt += 1) {
    try {
      const counts = getClusterSampleDatabaseCounts(state, selectedDatabases);
      if (counts.every((database) => database.count > 0)) {
        return counts;
      }
    } catch {
      // keep waiting
    }

    sleep(1000);
  }

  throw new Error("Timed out waiting for the selected sample databases to be imported into the cluster.");
}

function isSearchServiceRunning(state, serviceName) {
  return getComposeContainers(state).some(
    (container) => container.Service === serviceName && container.State === "running"
  );
}

function ensureSearchAssets(state) {
  ensureSearchEnabled(state);
  if (!dockerImageExists(SEARCH_MONGOT_IMAGE)) {
    runCommand("docker", ["pull", SEARCH_MONGOT_IMAGE]);
  }
  ensureClusterSampleArchive(state);
}

function ensureSearchReplicaPrimary(state) {
  ensureSearchEnabled(state);

  if (!state.topology.search.usesPrimaryNode) {
    for (let attempt = 0; attempt < 120; attempt += 1) {
      try {
        runMongoScript(
          state,
          SEARCH_SERVICE_NAME,
          `
const replicaConfig = {
  _id: ${JSON.stringify(SEARCH_REPLICA_SET)},
  members: [
    { _id: 0, host: ${JSON.stringify(`${SEARCH_SERVICE_NAME}:27017`)} }
  ]
};

try {
  const status = db.adminCommand({ replSetGetStatus: 1 });
  if (status.ok === 1) {
    print("Replica set already initialized");
  }
} catch (error) {
  if (error.code === 94) {
    printjson(rs.initiate(replicaConfig));
  } else {
    throw error;
  }
}
`.trim()
        );
        break;
      } catch {
        sleep(1000);
      }
    }

    for (let attempt = 0; attempt < 120; attempt += 1) {
      try {
        const result = runSearchMongoJson(
          state,
          `
const hello = db.hello();
const result = {
  isWritablePrimary: hello.isWritablePrimary === true || hello.ismaster === true
};
`.trim()
        );

        if (result.isWritablePrimary) {
          return;
        }
      } catch {
        // keep waiting
      }

      sleep(1000);
    }

    throw new Error("Timed out waiting for the Search node to become primary.");
  }

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      if (state.config.topology === "standalone") {
        runMongoScript(
          state,
          state.topology.standalone.serviceName,
          `
const replicaConfig = {
  _id: ${JSON.stringify(state.topology.search.replicaSet)},
  members: [
    { _id: 0, host: ${JSON.stringify(`${state.topology.standalone.serviceName}:27017`)} }
  ]
};

try {
  const status = db.adminCommand({ replSetGetStatus: 1 });
  if (status.ok === 1) {
    print("Replica set already initialized");
  }
} catch (error) {
  if (error.code === 94) {
    printjson(rs.initiate(replicaConfig));
  } else {
    throw error;
  }
}
`.trim()
        );
      }

      const serviceName = getSearchMongoServiceName(state, { writablePrimary: true });
      const hello = runMongoJson(
        state,
        serviceName,
        `
const hello = (db.hello && db.hello()) || db.isMaster();
const result = {
  isWritablePrimary: hello.isWritablePrimary === true || hello.ismaster === true
};
`.trim()
      );

      if (hello.isWritablePrimary) {
        return;
      }
    } catch {
      // keep waiting
    }

    sleep(1000);
  }
  throw new Error("Timed out waiting for Search support to become ready on the main MongoDB node.");
}

function ensureSearchCoordinatorUser(state) {
  ensureSearchEnabled(state);
  const serviceName = getSearchMongoServiceName(state, { writablePrimary: true });

  runMongoScript(
    state,
    serviceName,
    `
const adminDb = db.getSiblingDB("admin");
const username = "mongotUser";
const password = ${JSON.stringify(SEARCH_PASSWORD)};
const existing = adminDb.getUser(username);

if (!existing) {
  adminDb.createUser({
    user: username,
    pwd: password,
    roles: [{ role: "searchCoordinator", db: "admin" }]
  });
} else {
  adminDb.updateUser(username, {
    pwd: password,
    roles: [{ role: "searchCoordinator", db: "admin" }]
  });
}
`.trim()
  );
}

function getSearchSampleDatabases(state, requestedDatabases = state.config.sampleDatabases ?? []) {
  const available = new Set(getAvailableSampleDatabases());
  const selection = requestedDatabases.length ? requestedDatabases : state.config.sampleDatabases ?? [];

  for (const databaseName of selection) {
    if (!available.has(databaseName)) {
      throw new Error(`Unknown sample database '${databaseName}'.`);
    }
  }

  return selection;
}

function getSearchDatabaseCounts(state, databaseNames) {
  return databaseNames.map((databaseName) => ({
    databaseName,
    count: runMongoJson(
      state,
      getSearchMongoServiceName(state, { writablePrimary: true }),
      `
const result = {
  count: db.getSiblingDB(${JSON.stringify(databaseName)}).getCollectionNames().reduce((total, collectionName) => {
    return total + db.getSiblingDB(${JSON.stringify(databaseName)}).getCollection(collectionName).countDocuments({});
  }, 0)
};
`.trim()
    ).count
  }));
}

function ensureSampleDatabasesImported(state, requestedDatabases) {
  ensureSearchEnabled(state);
  const selectedDatabases = getSearchSampleDatabases(state, requestedDatabases);
  return ensureClusterSampleDatabasesImported(state, selectedDatabases);
}

function ensureSearchInfrastructure(state) {
  ensureSearchEnabled(state);
  ensureSearchAssets(state);

  ensureSearchReplicaPrimary(state);
  ensureSearchCoordinatorUser(state);

  if (!isSearchServiceRunning(state, MONGOT_SERVICE_NAME)) {
    printStep(
      state.topology.search.usesPrimaryNode ? 1 : 2,
      state.topology.search.usesPrimaryNode ? 1 : 2,
      "Start mongot",
      "Launching mongot for Search and vector operations."
    );
    runCompose(state, ["up", "-d", MONGOT_SERVICE_NAME]);
  }
}

function waitForSearchRuntime(state) {
  ensureSearchEnabled(state);

  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const result = runSearchMongoJson(
        state,
        `
const database = db.getSiblingDB("__mongodb_cli_lab_search_probe");
const collection = database.getCollection("probe");

try {
  database.createCollection("probe");
} catch (error) {
  if (error.codeName !== "NamespaceExists") {
    throw error;
  }
}

const indexes = collection.getSearchIndexes();
const result = { ready: Array.isArray(indexes) || typeof indexes?.toArray === "function" };
`.trim()
      );

      if (result.ready) {
        console.log("mongot is ready for Search operations.");
        return;
      }
    } catch {
      // keep waiting
    }

    if (attempt === 0 || attempt % 5 === 4) {
      console.log(`Waiting for mongot to become ready (${attempt + 1}s elapsed)...`);
    }

    sleep(1000);
  }

  throw new Error("Timed out waiting for mongot to become ready for Search operations.");
}

function ensureSearchIndex(state, databaseName, collectionName, indexName) {
  return runSearchMongoJson(
    state,
    `
const database = db.getSiblingDB(${JSON.stringify(databaseName)});
const collection = database.getCollection(${JSON.stringify(collectionName)});
const existing = collection
  .getSearchIndexes()
  .find((index) => index.name === ${JSON.stringify(indexName)});

let action = "reuse";
if (!existing) {
  collection.createSearchIndex(
    ${JSON.stringify(indexName)},
    { mappings: { dynamic: true } }
  );
  action = "create";
}

const result = { action };
`.trim()
  );
}

function waitForSearchIndexManagement(state, databaseName, collectionName, indexName) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      return ensureSearchIndex(state, databaseName, collectionName, indexName);
    } catch {
      sleep(1000);
    }
  }

  throw new Error("Timed out waiting for Search Index Management to become available.");
}

function waitForSearchQuery(state, databaseName, collectionName, query, pathName) {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const result = runSearchMongoJson(
        state,
        `
const documents = db.getSiblingDB(${JSON.stringify(databaseName)})
  .getCollection(${JSON.stringify(collectionName)})
  .aggregate([
    {
      $search: {
        text: {
          query: ${JSON.stringify(query)},
          path: ${JSON.stringify(pathName)}
        }
      }
    },
    { $limit: 5 },
    {
      $project: {
        _id: 0,
        title: 1,
        plot: 1,
        name: 1,
        summary: 1
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

    sleep(1000);
  }

  throw new Error("Timed out waiting for MongoDB Search to return results.");
}

function printSearchStatus(state, containers) {
  ensureSearchEnabled(state);
  const serviceNames = new Set([
    ...getSearchMongoServiceCandidates(state),
    state.topology.search.mongotServiceName
  ]);
  const searchContainers = containers.filter((container) => serviceNames.has(container.Service));

  console.log("\nSearch status\n");
  console.log(`MongoDB: ${getPrimaryConnectionString(state)}`);
  if (state.config.topology === "replica-set") {
    console.log(`Direct node access: ${getDirectConnectionString(state)}`);
  }
  console.log(`mongot: localhost:${state.topology.search.mongotPort}`);
  console.log(`Prepared sample databases: ${(state.config.sampleDatabases ?? []).join(", ") || "none"}\n`);

  if (!searchContainers.length) {
    console.log("Search services are not running.\n");
    return;
  }

  console.table(
    searchContainers.map((container) => ({
      service: container.Service,
      state: container.State,
      ports: formatPorts(container.Publishers)
    }))
  );
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

function getCollectionShardKeyCandidates(state, databaseName, collectionName) {
  return runMongoJson(
    state,
    state.topology.queryRouter.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const collection = db.getSiblingDB(databaseName).getCollection(collectionName);

function isPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date) && !(value instanceof ObjectId);
}

function collectPaths(value, prefix = "") {
  const paths = [];

  if (!isPlainObject(value)) {
    return paths;
  }

  for (const [key, entry] of Object.entries(value)) {
    const path = prefix ? prefix + "." + key : key;

    if (Array.isArray(entry)) {
      if (entry.length > 0 && isPlainObject(entry[0])) {
        continue;
      }
      paths.push(path);
      continue;
    }

    if (isPlainObject(entry)) {
      paths.push(...collectPaths(entry, path));
      continue;
    }

    paths.push(path);
  }

  return paths;
}

let sampleDocument = null;
try {
  sampleDocument = collection.findOne({});
} catch (error) {
  if (error.codeName !== "NamespaceNotFound" && !String(error.message || "").includes("ns does not exist")) {
    throw error;
  }
}

const fields = sampleDocument ? Array.from(new Set(["_id", ...collectPaths(sampleDocument)])).sort() : [];
const result = { fields, hasSample: Boolean(sampleDocument) };
`.trim()
  ).fields;
}

function getShardKeyIndexSupport(state, databaseName, collectionName, shardKeyField, shardKeyMode = "range") {
  return runMongoJson(
    state,
    state.topology.queryRouter.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const shardKeyField = ${JSON.stringify(shardKeyField)};
const shardKeyMode = ${JSON.stringify(shardKeyMode)};
const collection = db.getSiblingDB(databaseName).getCollection(collectionName);
const requiredKey = { [shardKeyField]: shardKeyMode === "hashed" ? "hashed" : 1 };

function isCompatibleIndex(indexKey) {
  const entries = Object.entries(indexKey || {});
  if (!entries.length) {
    return false;
  }

  const [firstField, firstValue] = entries[0];
  if (firstField !== shardKeyField) {
    return false;
  }

  if (shardKeyMode === "hashed") {
    return firstValue === "hashed";
  }

  return firstValue === 1 || firstValue === -1;
}

let indexes = [];
try {
  indexes = collection.getIndexes().map((index) => ({
    name: index.name,
    key: index.key
  }));
} catch (error) {
  if (error.codeName !== "NamespaceNotFound" && !String(error.message || "").includes("ns does not exist")) {
    throw error;
  }
}

const result = {
  requiredKey,
  indexes,
  hasCompatibleIndex: indexes.some((index) => isCompatibleIndex(index.key))
};
`.trim()
  );
}

function createShardKeyIndex(state, databaseName, collectionName, shardKeyField, shardKeyMode = "range") {
  return runMongoJson(
    state,
    state.topology.queryRouter.serviceName,
    `
const databaseName = ${JSON.stringify(databaseName)};
const collectionName = ${JSON.stringify(collectionName)};
const shardKeyField = ${JSON.stringify(shardKeyField)};
const shardKeyMode = ${JSON.stringify(shardKeyMode)};
const collection = db.getSiblingDB(databaseName).getCollection(collectionName);
const key = { [shardKeyField]: shardKeyMode === "hashed" ? "hashed" : 1 };
const indexName = collection.createIndex(key);
const result = {
  indexName,
  key
};
`.trim()
  );
}

async function ensureShardKeyIndex(state, databaseName, collectionName, shardKeyField, shardKeyMode = "range") {
  const indexSupport = getShardKeyIndexSupport(
    state,
    databaseName,
    collectionName,
    shardKeyField,
    shardKeyMode
  );

  if (indexSupport.hasCompatibleIndex) {
    return {
      created: false,
      key: indexSupport.requiredKey
    };
  }

  const existingIndexes = indexSupport.indexes.length
    ? indexSupport.indexes.map((index) => `${index.name}: ${JSON.stringify(index.key)}`).join("\n")
    : "No indexes found.";

  const confirmed = await confirmAction(
    [
      "MongoDB requires an index that starts with the proposed shard key before sharding this existing collection.",
      `Namespace: ${databaseName}.${collectionName}`,
      `Required index: ${JSON.stringify(indexSupport.requiredKey)}`,
      "",
      "Current indexes:",
      existingIndexes,
      "",
      "Create the required index now?"
    ].join("\n"),
    true
  );

  if (!confirmed) {
    throw new Error("Sharding cancelled because the required shard key index was not created.");
  }

  const createdIndex = createShardKeyIndex(
    state,
    databaseName,
    collectionName,
    shardKeyField,
    shardKeyMode
  );

  console.log(`Created index '${createdIndex.indexName}' with key ${JSON.stringify(createdIndex.key)}.\n`);

  return {
    created: true,
    key: createdIndex.key,
    indexName: createdIndex.indexName
  };
}

function getClusterOverview(state) {
  return runMongoJson(
    state,
    state.topology.queryRouter.serviceName,
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

function printClusterDataSummary(overview) {
  const databaseCount = overview.databases.length;
  const collectionCount = overview.collections.length;

  console.log("Data summary\n");
  console.log(`Databases: ${databaseCount}`);
  console.log(`Collections: ${collectionCount}\n`);

  if (!databaseCount) {
    console.log("No user databases found.\n");
    return;
  }

  for (const databaseName of overview.databases) {
    const collections = overview.collections
      .filter((collection) => collection.db === databaseName)
      .map((collection) => collection.name)
      .sort((left, right) => left.localeCompare(right));

    console.log(`- ${databaseName}: ${collections.length} collection(s)`);
    if (collections.length) {
      console.log(`  ${collections.join(", ")}`);
    }
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
  if (state.config.topology !== "sharded") {
    throw new Error("Sharding exercises are only available for sharded clusters.");
  }

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
    existingCollection: collectionAction.mode === "existing",
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
  console.log(`- create a ${config.topology} cluster`);
  if (config.topology === "sharded") {
    console.log(`- use ${config.shardCount} shard(s)`);
    console.log(`- use ${config.replicaSetMembers} replica set members per shard`);
  } else if (config.topology === "replica-set") {
    console.log(`- use ${config.replicaSetMembers} replica set members`);
  }
  console.log(`- run MongoDB ${config.mongodbVersion}`);
  console.log(`- expose MongoDB on localhost:${config.mongosPort}`);

  if (config.features?.search) {
    console.log("- enable MongoDB Search support");
    if (config.topology === "standalone" || config.topology === "replica-set") {
      console.log("- import sample_mflix");
      console.log('- create the "default" Search index on sample_mflix.movies');
      console.log('- run a $search query for "baseball"\n');
      return;
    }
  }

  if (config.topology === "sharded") {
    console.log("- create the demo collection library.books");
    console.log('- shard the collection by { "_id": "hashed" }');
    console.log("- insert 500 sample documents");
    console.log("- show how documents were distributed across shards\n");
    return;
  }

  console.log("- create and start the cluster with no extra demo steps\n");
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
  let shardKeyCandidates = [];

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
  } else {
    currentDocumentCount = getCollectionDocumentCount(
      state,
      target.databaseName,
      target.collectionName
    );
  }

  if (currentDocumentCount > 0) {
    shardKeyCandidates = getCollectionShardKeyCandidates(
      state,
      target.databaseName,
      target.collectionName
    );

    console.log(
      `\nFound ${currentDocumentCount} existing document(s) in ${target.databaseName}.${target.collectionName}.`
    );
    if (shardKeyCandidates.length) {
      console.log(`Available shard key field candidates: ${shardKeyCandidates.join(", ")}\n`);
    } else {
      console.log("Could not infer field candidates from existing documents. You can enter a field manually.\n");
    }
  }

  let answers;
  let insertCount = 0;
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
        type: shardKeyCandidates.length ? "list" : "input",
        name: "shardKeyField",
        message: currentDocumentCount > 0 ? "Choose a shard key field from the existing collection:" : "Shard key field:",
        choices: shardKeyCandidates.length
          ? [
              ...shardKeyCandidates.map((field) => ({ name: field, value: field })),
              { name: "Enter a custom field", value: "__custom__" },
              { name: "Back", value: "back" }
            ]
          : undefined,
        default: shardKeyCandidates.length ? "_id" : "_id",
        when: (answers) => answers.continue === "continue",
        validate: (value) =>
          value.trim().toLowerCase() === "back" || value.trim() ? true : "Shard key field cannot be empty."
      },
      {
        type: "input",
        name: "customShardKeyField",
        message: "Enter the shard key field:",
        default: "_id",
        when: (answers) => answers.continue === "continue" && answers.shardKeyField === "__custom__",
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
        message: currentDocumentCount > 0
          ? "How many extra demo documents should be inserted after sharding?"
          : "How many demo documents should be inserted?",
        choices: [
          { name: "0 documents", value: 0 },
          { name: "20 documents", value: 20 },
          { name: "100 documents", value: 100 },
          { name: "1000 documents", value: 1000 },
          { name: "Custom", value: "custom" },
          { name: "Back", value: "back" }
        ],
        default: mode === "demo" ? 20 : 0,
        when: (answers) => answers.continue === "continue" && !target.existingCollection,
      }
    ]);

    if (
      answers.continue === "back" ||
      answers.shardKeyField?.trim?.().toLowerCase() === "back" ||
      answers.customShardKeyField?.trim?.().toLowerCase() === "back" ||
      answers.shardKeyMode === "back" ||
      answers.insertCountChoice === "back"
    ) {
      return;
    }

    if (target.existingCollection) {
      break;
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
    const selectedShardKeyField = answers.shardKeyField === "__custom__"
      ? answers.customShardKeyField.trim()
      : answers.shardKeyField.trim();
    const requestedShardKey = formatShardKey(
      selectedShardKeyField,
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

  if (!target.existingCollection) {
    printInsertPlan(`${target.databaseName}.${target.collectionName}`, insertCount);
  }
  const selectedShardKeyField = answers.shardKeyField === "__custom__"
    ? answers.customShardKeyField.trim()
    : answers.shardKeyField.trim();
  if (!resetCollection && currentDocumentCount > 0) {
    await ensureShardKeyIndex(
      state,
      target.databaseName,
      target.collectionName,
      selectedShardKeyField,
      answers.shardKeyMode
    );
  }

  const result = shardCollection(state, {
    databaseName: target.databaseName,
    collectionName: target.collectionName,
    shardKeyField: selectedShardKeyField,
    shardKeyMode: answers.shardKeyMode,
    documents: [],
    resetCollection,
    skipInsert: true
  });
  const inserted = target.existingCollection
    ? 0
    : insertDocumentsInBatches(state, {
      databaseName: target.databaseName,
      collectionName: target.collectionName,
      shardKeyField: selectedShardKeyField,
      insertCount,
      startIndex: currentDocumentCount,
      seedMode: "sample-generated"
    });

  console.log("\nCollection updated\n");
  console.log(`Namespace: ${result.namespace}`);
  console.log(`Previous shard key: ${JSON.stringify(result.previousShardKey)}`);
  console.log(`Shard key: ${JSON.stringify(result.shardKey)}`);
  console.log(`Action: ${result.actionTaken}`);
  if (!target.existingCollection) {
    console.log(`Documents inserted: ${inserted}`);
    console.log("Insert completed.");
  }
  console.log(`Shard result: ${JSON.stringify(result.shardResult)}\n`);
}

async function interactiveShardingMenu() {
  const state = await loadState();
  if (!state) {
    console.log("\nNo cluster has been configured yet.\n");
    return;
  }

  if (state.config.topology !== "sharded") {
    console.log("\nSharding exercises are only available for sharded clusters.\n");
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
  const shouldImportSampleData = Boolean(state.config.sampleDatabases?.length);
  const totalSteps = (state.config.topology === "sharded" ? 5 : 2) +
    (shouldImportSampleData ? 1 : 0) +
    (state.topology.search ? 1 : 0);

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
  if (shouldImportSampleData) {
    ensureClusterSampleArchive(state);
  }

  let nextStep = 2;
  if (state.config.topology === "standalone") {
    printStep(2, totalSteps, "Start standalone node", "Starting a single MongoDB node.");
    runCompose(state, ["up", "-d", state.topology.standalone.serviceName]);
    waitForMongo(state, state.topology.standalone.serviceName);
    if (state.topology.search?.usesPrimaryNode) {
      ensureSearchReplicaPrimary(state);
    }
    nextStep = 3;
  } else if (state.config.topology === "replica-set") {
    printStep(2, totalSteps, "Start replica set members", "Starting MongoDB replica set members.");
    runCompose(state, ["up", "-d", ...state.topology.replicaSet.members.map((member) => member.serviceName)]);
    waitForServices(state, state.topology.replicaSet.members.map((member) => member.serviceName));
    printStep(3, totalSteps, "Initialize replica set", "Electing a primary for the replica set.");
    runMongoScript(
      state,
      state.topology.replicaSet.members[0].serviceName,
      buildReplicaInitScript(buildReplicaSetConfig(state.topology.replicaSet.name, state.topology.replicaSet.members))
    );
    if (state.topology.search?.usesPrimaryNode) {
      ensureSearchReplicaPrimary(state);
    }
    nextStep = 4;
  } else {
    const configServices = state.topology.configServers.map((member) => member.serviceName);
    const shardServices = state.topology.shards.flatMap((shard) =>
      shard.members.map((member) => member.serviceName)
    );

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
    nextStep = 6;
  }

  if (shouldImportSampleData) {
    printStep(
      nextStep,
      totalSteps,
      "Import sample databases",
      `Restoring the selected sample databases into the ${state.config.topology} cluster.`
    );
    const counts = ensureClusterSampleDatabasesImported(state, state.config.sampleDatabases);
    console.log(`Imported: ${counts.map((database) => `${database.databaseName} (${database.count})`).join(", ")}`);
    nextStep += 1;
  }

  if (state.topology.search) {
    printStep(
      nextStep,
      totalSteps,
      "Start Search services",
      "Enabling Search on the main MongoDB node and launching mongot in the same lab environment."
    );
    ensureSearchInfrastructure(state);
  }
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
        `Topology: ${config.topology}`,
        ...(config.topology === "sharded"
          ? [`Shards: ${config.shardCount}`, `Members per shard: ${config.replicaSetMembers}`]
          : config.topology === "replica-set"
            ? [`Replica set members: ${config.replicaSetMembers}`]
            : []),
        `MongoDB version: ${config.mongodbVersion}`,
        `MongoDB port: ${config.mongosPort}`,
        `Search support: ${config.features?.search ? "enabled" : "disabled"}`,
        `Sample databases: ${(config.sampleDatabases ?? []).join(", ") || "none"}`,
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

  if (!desiredConfig) {
    return null;
  }

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

  if (state.config.topology === "sharded" && state.config.features?.search) {
    throw new Error("Search is not implemented on sharded clusters in this CLI yet. Recreate the cluster without Search or use standalone/replica-set.");
  }

  await bringUpCluster(state);

  console.log("\nCluster ready\n");
  printTopologyDiagram(state);
  console.log("Connection strings:");
  console.log(getPrimaryConnectionString(state));
  if (state.config.topology === "replica-set") {
    console.log(`Direct node access: ${getDirectConnectionString(state)}`);
    if (state.config.features?.search) {
      console.log("Replica set discovery for host clients is disabled when Search is enabled. Use direct node access.");
    }
  }
  if (state.topology.search) {
    console.log("Search is available on the main MongoDB connection above.");
    console.log(`mongot: localhost:${state.topology.search.mongotPort}`);
  }
  console.log("\nIf initialization is interrupted, rerunning 'up' will retry safely.\n");
  return state;
}

async function runSearchUp(options = {}) {
  const existingState = await loadState();
  if (!existingState) {
    return runUp({ ...options, search: true });
  }

  if (existingState.config.topology === "sharded") {
    throw new Error("Search is not implemented on sharded clusters in this CLI yet. Use standalone or replica-set.");
  }

  ensureSearchEnabled(existingState);
  ensureDockerAvailable();
  ensureSearchInfrastructure(existingState);
  return existingState;
}

async function runSearchStatus() {
  ensureDockerAvailable();
  const state = await requireState();
  ensureSearchEnabled(state);
  printSearchStatus(state, getComposeContainers(state));
}

async function runSearchImportDatabases(options = {}) {
  ensureDockerAvailable();
  const state = await requireState();
  ensureSearchEnabled(state);
  ensureSearchInfrastructure(state);

  const databaseNames = options.all
    ? getAvailableSampleDatabases()
    : options.databaseNames ?? state.config.sampleDatabases ?? [];

  const counts = ensureSampleDatabasesImported(state, databaseNames);
  if (databaseNames.length) {
    state.config.sampleDatabases = [...new Set(databaseNames)];
    await saveState(state);
  }

  console.log("\nImported sample databases\n");
  for (const database of counts) {
    console.log(`- ${database.databaseName}: ${database.count} documents`);
  }
  console.log("");
}

async function runSearchQuickstart(options = {}) {
  const state = await runSearchUp(options);
  if (!state) {
    return;
  }

  const databaseName = "sample_mflix";
  const collectionName = "movies";
  const indexName = "default";
  const query = "baseball";
  const pathName = "plot";

  printStep(1, 3, "Import sample data", "Importing sample_mflix into the main MongoDB node with Search enabled.");
  await runSearchImportDatabases({ databaseNames: [databaseName] });

  printStep(2, 3, "Create Search index", "Creating the default Search index on sample_mflix.movies.");
  waitForSearchRuntime(state);
  const indexResult = waitForSearchIndexManagement(state, databaseName, collectionName, indexName);

  printStep(3, 3, "Run Search query", "Running a sample $search query for 'baseball'.");
  const documents = waitForSearchQuery(state, databaseName, collectionName, query, pathName);

  console.log("\nMongoDB Search quickstart completed\n");
  console.log(`Index action: ${indexResult.action}`);
  console.log(`Namespace: ${databaseName}.${collectionName}`);
  console.log(`Search index: ${indexName}\n`);
  console.log("Sample results:\n");
  for (const document of documents) {
    console.log(JSON.stringify(document, null, 2));
  }
  console.log("\nTry this query in mongosh:\n");
  console.log(`use ${databaseName}`);
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
  console.log(`Connect with: mongosh "${state.config.topology === "replica-set" ? getDirectConnectionString(state) : getPrimaryConnectionString(state)}"\n`);
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
  const overview = getClusterOverview(state);
  printClusterDataSummary(overview);
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
        message: `Manage cluster\n${runningCount}/${countExpectedNodes(state)} nodes are currently running`,
        choices: [
          { name: "1. Show cluster details", value: "show" },
          { name: "2. Start cluster", value: "up" },
          { name: "3. Stop cluster", value: "down" },
          { name: "4. Delete cluster and files", value: "clean" },
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

async function promptSearchDatabaseSelection() {
  const availableDatabases = getAvailableSampleDatabases();
  const noneValue = "__none__";
  const allValue = "__all__";

  const { selections } = await inquirer.prompt([
    {
      type: "checkbox",
      name: "selections",
      message: "Choose which sample databases to import into the Search lab",
      choices: [
        { name: "None", value: noneValue },
        { name: "All available sample databases", value: allValue },
        ...availableDatabases.map((database) => ({
          name: database,
          value: database
        }))
      ],
      validate(value) {
        return value.length > 0 ? true : "Select at least one database to continue.";
      }
    }
  ]);

  if (selections.includes(noneValue)) {
    return {
      all: false,
      databaseNames: []
    };
  }

  if (selections.includes(allValue)) {
    return {
      all: true,
      databaseNames: [...availableDatabases]
    };
  }

  return {
    all: false,
    databaseNames: selections
  };
}

async function interactiveSearchMenu() {
  let exitMenu = false;

  while (!exitMenu) {
    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: "Search\nUse the Search services attached to this cluster",
        choices: [
          { name: "1. Start Search services", value: "up" },
          { name: "2. Show Search status", value: "status" },
          { name: "3. Import sample databases", value: "import" },
          { name: "4. Run Search quickstart demo", value: "quickstart" },
          { name: "5. Back", value: "back" }
        ],
        default: "up"
      }
    ]);

    if (action === "up") {
      await runSearchUp();
      continue;
    }

    if (action === "status") {
      await runSearchStatus();
      continue;
    }

    if (action === "import") {
      const selection = await promptSearchDatabaseSelection();
      await runSearchImportDatabases(selection);
      continue;
    }

    if (action === "quickstart") {
      await runSearchQuickstart();
      continue;
    }

    exitMenu = true;
  }
}

function parseMongoMajorMinor(version) {
  const match = String(version ?? "").match(/^(\d+)(?:\.(\d+))?/);
  if (!match) {
    return null;
  }

  return {
    major: Number(match[1]),
    minor: Number(match[2] ?? 0)
  };
}

function supportsSearchVersion(version) {
  const parsed = parseMongoMajorMinor(version);
  if (!parsed) {
    return false;
  }

  return parsed.major > 8 || (parsed.major === 8 && parsed.minor >= 2);
}

async function ensureClusterReadyForSearch() {
  const state = await loadState();

  if (!state) {
    console.log("\nSet up a cluster first. MongoDB Search requires a configured lab environment.\n");
    return false;
  }

  if (!supportsSearchVersion(state.config.mongodbVersion)) {
    console.log(
      [
        "",
        `Search lab requires MongoDB 8.2 or newer.`,
        `Current cluster version: ${state.config.mongodbVersion}`,
        "Set up the cluster again with MongoDB 8.2+ before using Search.",
        ""
      ].join("\n")
    );
    return false;
  }

  if (!state.config.features?.search) {
    console.log(
      [
        "",
        "This cluster was created without Search support.",
        "Set up the cluster again with Search enabled before using the Search menu.",
        ""
      ].join("\n")
    );
    return false;
  }

  if (state.config.topology === "sharded") {
    console.log(
      [
        "",
        "Search is not enabled for sharded clusters in this CLI yet.",
        "Use a standalone or replica-set topology for Search support for now.",
        ""
      ].join("\n")
    );
    return false;
  }

  return true;
}

async function interactiveMainMenu() {
  ensureDockerAvailable();

  let exitMenu = false;

  while (!exitMenu) {
    const activeState = await loadActiveClusterState();
    const savedState = activeState ?? await loadState();
    const stateLabel = activeState
      ? `${activeState.config.topology} cluster ready on MongoDB ${activeState.config.mongodbVersion}${activeState.config.features?.search ? " with Search" : ""}`
      : savedState
        ? `${savedState.config.topology} cluster saved with MongoDB ${savedState.config.mongodbVersion}${savedState.config.features?.search ? " and Search support" : ""}`
        : "No cluster configured yet";

    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: `MongoDB CLI Lab\n${stateLabel}\nChoose what you want to do next`,
        choices: [
          { name: "1. Set up cluster", value: "up" },
          { name: "2. Search lab", value: "search" },
          { name: "3. Work with data and sharding", value: "collections" },
          { name: "4. Manage cluster", value: "manage" },
          { name: "5. Exit", value: "exit" }
        ],
        default: "up"
      }
    ]);

    if (action === "up") {
      await runUp();
      continue;
    }

    if (action === "search") {
      try {
        if (await ensureClusterReadyForSearch()) {
          await interactiveSearchMenu();
        }
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

  if (state.config.features?.search && (state.config.topology === "standalone" || state.config.topology === "replica-set")) {
    await runSearchQuickstart({ ...options, confirm: false });
    return;
  }

  if (state.config.topology === "sharded") {
    await runQuickstartDemo(state);
    return;
  }

  console.log("\nQuickstart setup completed\n");
}

export {
  interactiveMainMenu,
  runClean,
  runDown,
  runQuickstart,
  runSearchImportDatabases,
  runSearchQuickstart,
  runSearchStatus,
  runSearchUp,
  runStatus,
  runUp
};
