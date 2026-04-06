import crypto from "node:crypto";
import { existsSync } from "node:fs";
import fs from "node:fs/promises";
import path from "node:path";
import { execSync } from "node:child_process";
import inquirer from "inquirer";
import { Binary, ClientEncryption, Double, Int32, MongoClient } from "mongodb";

async function promptAndInstallEncryptionPackage() {
  console.log("\nThe 'mongodb-client-encryption' package is required for Queryable Encryption but is not installed.");
  const { install } = await inquirer.prompt([
    {
      type: "confirm",
      name: "install",
      message: "Do you want to install it now? (npm install -g mongodb-client-encryption)",
      default: true
    }
  ]);

  if (!install) {
    throw new Error("Queryable Encryption requires 'mongodb-client-encryption'. Aborting.");
  }

  console.log("\nInstalling mongodb-client-encryption...");
  try {
    execSync("npm install -g mongodb-client-encryption", { stdio: "inherit" });
    console.log("\nInstallation complete. Please run the command again to continue.");
  } catch {
    console.log("\nInstallation failed. Please run manually: npm install -g mongodb-client-encryption");
  }
  process.exit(0);
}

function isMissingEncryptionPackageError(error) {
  return error?.code === "ERR_MODULE_NOT_FOUND"
    || error?.name === "MongoMissingDependencyError"
    || (typeof error?.message === "string" && error.message.includes("mongodb-client-encryption"));
}

const MASTER_KEY_BYTES = 96;
const QE_DIRECTORY_NAME = "queryable-encryption";
const QE_STATE_FILE_NAME = "state.json";
const MASTER_KEY_FILE_NAME = "local-master-key.bin";
const KEY_VAULT_DATABASE = "encryption";
const KEY_VAULT_COLLECTION = "__keyVault";
const DEFAULT_QE_DATABASE = "qe_lab";
const DEFAULT_QE_COLLECTION = "employees";
const QUERYABLE_ENCRYPTION_RESOURCES = Object.freeze([
  {
    label: "Queryable Encryption overview",
    url: "https://www.mongodb.com/docs/manual/core/queryable-encryption/?utm_campaign=devrel&utm_source=third-part-content&utm_medium=cta&utm_content=mongodb-cli-lab&utm_term=ricardo.mello"
  },
  {
    label: "Security and in-use encryption",
    url: "https://www.mongodb.com/docs/manual/core/security-in-use-encryption/?utm_campaign=devrel&utm_source=third-part-content&utm_medium=cta&utm_content=mongodb-cli-lab&utm_term=ricardo.mello"
  },
  {
    label: "Queryable Encryption with Spring Data MongoDB",
    url: "https://dev.to/mongodb/queryable-encryption-with-spring-data-mongodb-how-to-query-encrypted-fields-2ccc?utm_campaign=devrel&utm_source=third-part-content&utm_medium=cta&utm_content=mongodb-cli-lab&utm_term=ricardo.mello"
  }
]);
const DEMO_FIELDS = Object.freeze([
  { name: "ssn", bsonType: "string", queryType: "equality" },
  { name: "email", bsonType: "string", queryType: "equality" },
  { name: "department", bsonType: "string", queryType: "equality" },
  {
    name: "salary",
    bsonType: "int",
    queryType: "range",
    min: new Int32(30000),
    max: new Int32(200000),
    sparsity: 1,
    trimFactor: 6
  }
]);
const DEMO_DOCUMENTS = Object.freeze([
  {
    name: "Alice Johnson",
    ssn: "111-22-3333",
    email: "alice@example.com",
    department: "Sales",
    city: "Austin",
    salary: new Int32(65000)
  },
  {
    name: "Bruno Costa",
    ssn: "222-33-4444",
    email: "bruno@example.com",
    department: "Finance",
    city: "New York",
    salary: new Int32(98000)
  },
  {
    name: "Carla Mendes",
    ssn: "333-44-5555",
    email: "carla@example.com",
    department: "Engineering",
    city: "Sao Paulo",
    salary: new Int32(135000)
  }
]);

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

function supportsQueryableEncryptionVersion(version) {
  const parsed = parseMongoMajorMinor(version);
  if (!parsed) {
    return false;
  }

  return parsed.major > 7 || (parsed.major === 7 && parsed.minor >= 0);
}

function supportsQueryableEncryptionRangeQueries(version) {
  const parsed = parseMongoMajorMinor(version);
  if (!parsed) {
    return false;
  }

  return parsed.major > 8 || (parsed.major === 8 && parsed.minor >= 0);
}

function getAvailableDemoFields(state) {
  const supportsRange = supportsQueryableEncryptionRangeQueries(state?.config?.mongodbVersion);
  return DEMO_FIELDS.filter((field) => supportsRange || field.queryType !== "range");
}

function ensureQueryableEncryptionCompatible(state) {
  if (!state) {
    throw new Error("Set up a cluster first. Queryable Encryption requires a configured lab environment.");
  }

  if (!supportsQueryableEncryptionVersion(state.config.mongodbVersion)) {
    throw new Error(`Queryable Encryption lab requires MongoDB 7.0 or newer. Current cluster version: ${state.config.mongodbVersion}`);
  }

  if (state.config.topology !== "replica-set") {
    throw new Error("Queryable Encryption lab currently supports replica-set topologies only.");
  }
}

export function getQueryableEncryptionCompatibilityError(state) {
  if (!state) {
    return "Set up a cluster first. Queryable Encryption requires a configured lab environment.";
  }

  if (!supportsQueryableEncryptionVersion(state.config.mongodbVersion)) {
    return `Queryable Encryption lab requires MongoDB 7.0 or newer. Current cluster version: ${state.config.mongodbVersion}`;
  }

  if (state.config.topology !== "replica-set") {
    return "Queryable Encryption lab currently supports replica-set topologies only.";
  }

  return null;
}

function getQeDirectory(state) {
  return path.join(state.config.storagePath, QE_DIRECTORY_NAME);
}

function getQeStateFilePath(state) {
  return path.join(getQeDirectory(state), QE_STATE_FILE_NAME);
}

function getMasterKeyFilePath(state) {
  return path.join(getQeDirectory(state), MASTER_KEY_FILE_NAME);
}

function getKeyVaultNamespace() {
  return `${KEY_VAULT_DATABASE}.${KEY_VAULT_COLLECTION}`;
}

async function ensureQeDirectory(state) {
  await fs.mkdir(getQeDirectory(state), { recursive: true });
}

async function ensureLocalMasterKey(state) {
  await ensureQeDirectory(state);
  const filePath = getMasterKeyFilePath(state);

  if (existsSync(filePath)) {
    return fs.readFile(filePath);
  }

  const key = crypto.randomBytes(MASTER_KEY_BYTES);
  await fs.writeFile(filePath, key);
  return key;
}

function getClusterConnectionString(state) {
  if (state.config.topology === "standalone") {
    return `mongodb://localhost:${state.config.mongosPort}/?directConnection=true`;
  }

  const hosts = state.topology.replicaSet.members
    .map((member) => member.externalHost ?? `${member.advertisedHostname}:${member.hostPort}`)
    .join(",");
  return `mongodb://${hosts}/?replicaSet=${state.topology.replicaSet.name}`;
}

async function createQeClients(state, encryptedFieldsMap = undefined) {
  const localMasterKey = await ensureLocalMasterKey(state);
  const kmsProviders = {
    local: {
      key: localMasterKey
    }
  };
  const keyVaultNamespace = getKeyVaultNamespace();
  const uri = getClusterConnectionString(state);

  const regularClient = new MongoClient(uri);
  await regularClient.connect();

  const qeClient = new MongoClient(uri, {
    autoEncryption: {
      keyVaultNamespace,
      kmsProviders,
      encryptedFieldsMap
    }
  });

  try {
    await qeClient.connect();
  } catch (error) {
    await regularClient.close();
    await qeClient.close();
    if (isMissingEncryptionPackageError(error)) {
      await promptAndInstallEncryptionPackage();
    }
    throw error;
  }

  let clientEncryption;
  try {
    clientEncryption = new ClientEncryption(regularClient, {
      keyVaultNamespace,
      keyVaultClient: regularClient,
      kmsProviders
    });
  } catch (error) {
    await regularClient.close();
    await qeClient.close();
    if (isMissingEncryptionPackageError(error)) {
      await promptAndInstallEncryptionPackage();
    }
    throw error;
  }

  return {
    regularClient,
    qeClient,
    clientEncryption,
    keyVaultNamespace,
    uri
  };
}

async function recreateQeClient(state, clients, encryptedFieldsMap) {
  if (clients.qeClient) {
    await clients.qeClient.close();
  }

  const localMasterKey = await ensureLocalMasterKey(state);
  clients.qeClient = new MongoClient(clients.uri, {
    autoEncryption: {
      keyVaultNamespace: clients.keyVaultNamespace,
      kmsProviders: {
        local: {
          key: localMasterKey
        }
      },
      encryptedFieldsMap
    }
  });
  await clients.qeClient.connect();
}

async function ensureKeyVaultIndexes(client) {
  await client
    .db(KEY_VAULT_DATABASE)
    .collection(KEY_VAULT_COLLECTION)
    .createIndex(
      { keyAltNames: 1 },
      {
        unique: true,
        partialFilterExpression: { keyAltNames: { $exists: true } }
      }
    );
}

function summarizeValue(value) {
  if (value instanceof Int32) {
    return value.valueOf();
  }

  if (value instanceof Double) {
    return value.valueOf();
  }

  if (value instanceof Binary) {
    return {
      _type: "Binary",
      subtype: value.sub_type,
      bytes: value.buffer?.length ?? value.length?.() ?? 0
    };
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value)) {
    return value.map((entry) => summarizeValue(entry));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, summarizeValue(entry)])
    );
  }

  return value;
}

function stripInternalQueryFields(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => stripInternalQueryFields(entry));
  }

  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => key !== "_id" && key !== "__safeContent__")
        .map(([key, entry]) => [key, stripInternalQueryFields(entry)])
    );
  }

  return value;
}

function getSelectedDemoFieldDefinitions(state, fieldNames) {
  return getAvailableDemoFields(state).filter((field) => fieldNames.includes(field.name));
}

function getFieldDefinitionsForFlow(state, options) {
  if (options.fieldDefinitions?.length) {
    return options.fieldDefinitions;
  }

  return getSelectedDemoFieldDefinitions(state, options.encryptedFieldNames ?? []);
}

function normalizeRangeBound(value, bsonType) {
  if (value === null || value === undefined) {
    return value;
  }

  if (bsonType === "int") {
    return value instanceof Int32 ? value : new Int32(Number(value));
  }

  if (bsonType === "double") {
    return value instanceof Double ? value : new Double(Number(value));
  }

  if (bsonType === "date") {
    return value instanceof Date ? value : new Date(value);
  }

  return value;
}

function parseEncryptedFieldDefinitions(encryptedFields) {
  return (encryptedFields?.fields ?? []).map((field) => ({
    name: field.path,
    bsonType: field.bsonType,
    queryType: field.queries?.queryType === "range"
      ? "range"
      : field.queries?.queryType === "equality"
        ? "equality"
        : null,
    ...(field.queries?.queryType === "range"
      ? {
          min: field.queries.min,
          max: field.queries.max,
          ...(field.queries.precision !== undefined ? { precision: field.queries.precision } : {}),
          sparsity: field.queries.sparsity,
          trimFactor: field.queries.trimFactor
        }
      : {})
  }));
}

function normalizeEncryptedFieldsDocument(encryptedFields) {
  if (!encryptedFields?.fields) {
    return encryptedFields;
  }

  return {
    ...encryptedFields,
    fields: encryptedFields.fields.map((field) => ({
      ...field,
      ...(field.queries?.queryType === "range"
        ? {
            queries: {
              ...field.queries,
              min: normalizeRangeBound(field.queries.min, field.bsonType),
              max: normalizeRangeBound(field.queries.max, field.bsonType)
            }
          }
        : {})
    }))
  };
}

function buildEncryptedFieldsDocument(fieldDefinitions) {
  return {
    fields: fieldDefinitions.map((field) => ({
      path: field.name,
      bsonType: field.bsonType,
      ...(field.queryType
        ? {
            queries: field.queryType === "range"
              ? {
                  queryType: "range",
                  min: field.min,
                  max: field.max,
                  ...(field.precision !== undefined ? { precision: field.precision } : {}),
                  sparsity: field.sparsity,
                  trimFactor: field.trimFactor
                }
              : { queryType: "equality" }
          }
        : {})
    }))
  };
}

async function dropCollectionIfExists(client, databaseName, collectionName) {
  try {
    await client.db(databaseName).collection(collectionName).drop();
  } catch (error) {
    if (error?.codeName !== "NamespaceNotFound" && !String(error?.message || "").includes("ns not found")) {
      throw error;
    }
  }
}

async function collectionExists(client, databaseName, collectionName) {
  const collectionInfo = await client
    .db(databaseName)
    .listCollections({ name: collectionName }, { nameOnly: true })
    .next();

  return Boolean(collectionInfo);
}

async function createEncryptedCollection(state, options) {
  const {
    databaseName,
    collectionName,
    resetCollection
  } = options;
  const fieldDefinitions = getFieldDefinitionsForFlow(state, options);
  if (!fieldDefinitions.length) {
    throw new Error("Choose at least one field to encrypt.");
  }

  const encryptedFields = buildEncryptedFieldsDocument(fieldDefinitions);
  const encryptedFieldsMap = {
    [`${databaseName}.${collectionName}`]: encryptedFields
  };

  const clients = await createQeClients(state, encryptedFieldsMap);
  let backupCollectionName = null;

  try {
    await ensureKeyVaultIndexes(clients.regularClient);
    if (resetCollection && await collectionExists(clients.regularClient, databaseName, collectionName)) {
      backupCollectionName = `${collectionName}__qe_backup_${Date.now()}`;
      await renameCollection(
        clients.regularClient,
        databaseName,
        collectionName,
        backupCollectionName
      );
    }

    const { encryptedFields: createdEncryptedFields } = await clients.clientEncryption.createEncryptedCollection(
      clients.regularClient.db(databaseName),
      collectionName,
      {
        provider: "local",
        createCollectionOptions: {
          encryptedFields
        }
      }
    );

    return {
      clients,
      encryptedFields: createdEncryptedFields,
      backupCollectionName
    };
  } catch (error) {
    if (backupCollectionName) {
      try {
        await dropCollectionIfExists(clients.regularClient, databaseName, collectionName);
        await renameCollection(
          clients.regularClient,
          databaseName,
          backupCollectionName,
          collectionName
        );
      } catch {
        // Preserve the original error; callers surface the failure.
      }
    }

    await clients.regularClient.close();
    await clients.qeClient.close();
    throw error;
  }
}

function formatScalarForDisplay(value) {
  if (value instanceof Int32) {
    return value.valueOf();
  }

  return value;
}

function getFieldQuerySummary(fieldName) {
  return DEMO_FIELDS.find((field) => field.name === fieldName) ?? null;
}

function getQueryTypeLabel(queryType) {
  return queryType ?? "encrypted-only";
}

function supportsRangeForBsonType(bsonType) {
  return ["int", "double", "date"].includes(bsonType);
}

function inferFieldDefinitionFromValue(fieldName, value, state) {
  if (value === null || value === undefined || fieldName === "_id" || fieldName === "__safeContent__") {
    return null;
  }

  if (value instanceof Date) {
    return {
      name: fieldName,
      bsonType: "date",
      queryType: supportsQueryableEncryptionRangeQueries(state.config.mongodbVersion) ? "range" : "equality",
      min: new Date("2000-01-01T00:00:00.000Z"),
      max: new Date("2100-01-01T00:00:00.000Z"),
      sparsity: 1,
      trimFactor: 6
    };
  }

  if (typeof value === "string") {
    return { name: fieldName, bsonType: "string", queryType: "equality" };
  }

  if (typeof value === "boolean") {
    return { name: fieldName, bsonType: "bool", queryType: "equality" };
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    if (Number.isInteger(value)) {
      return {
        name: fieldName,
        bsonType: "int",
        queryType: supportsQueryableEncryptionRangeQueries(state.config.mongodbVersion) ? "range" : "equality",
        min: new Int32(-2147483648),
        max: new Int32(2147483647),
        sparsity: 1,
        trimFactor: 6
      };
    }

    return {
      name: fieldName,
      bsonType: "double",
      queryType: supportsQueryableEncryptionRangeQueries(state.config.mongodbVersion) ? "range" : "equality",
      min: -1_000_000_000,
      max: 1_000_000_000,
      sparsity: 1,
      trimFactor: 6
    };
  }

  return null;
}

function mergeInferredFieldDefinition(existingDefinition, nextDefinition) {
  if (!existingDefinition) {
    return { ...nextDefinition };
  }

  if (existingDefinition.bsonType !== nextDefinition.bsonType) {
    return null;
  }

  return existingDefinition;
}

function coerceValueForField(value, fieldDefinition) {
  if (value === null || value === undefined) {
    return value;
  }

  if (fieldDefinition.bsonType === "int") {
    return value instanceof Int32 ? value : new Int32(Number(value));
  }

  if (fieldDefinition.bsonType === "double") {
    return value instanceof Double ? value : new Double(Number(value));
  }

  if (fieldDefinition.bsonType === "date") {
    return value instanceof Date ? value : new Date(value);
  }

  return value;
}

function validateInputForBsonType(value, bsonType) {
  const trimmed = String(value ?? "").trim();
  if (!trimmed) {
    return "Value cannot be empty.";
  }

  if (bsonType === "int") {
    return /^-?\d+$/.test(trimmed) ? true : "Enter a valid integer.";
  }

  if (bsonType === "double") {
    return Number.isFinite(Number(trimmed)) ? true : "Enter a valid number.";
  }

  if (bsonType === "date") {
    return Number.isNaN(Date.parse(trimmed)) ? "Enter a valid ISO date or date-time." : true;
  }

  return true;
}

function parseInputForBsonType(value, bsonType) {
  const trimmed = String(value ?? "").trim();

  if (bsonType === "int") {
    return new Int32(Number(trimmed));
  }

  if (bsonType === "double") {
    return new Double(Number(trimmed));
  }

  if (bsonType === "date") {
    return new Date(trimmed);
  }

  return trimmed;
}

async function promptQueryValue(message, bsonType) {
  if (bsonType === "bool") {
    const { value } = await inquirer.prompt([
      {
        type: "list",
        name: "value",
        message,
        choices: [
          { name: "true", value: true },
          { name: "false", value: false }
        ],
        default: true
      }
    ]);

    return value;
  }

  const { value } = await inquirer.prompt([
    {
      type: "input",
      name: "value",
      message,
      validate: (input) => validateInputForBsonType(input, bsonType)
    }
  ]);

  return parseInputForBsonType(value, bsonType);
}

function buildRangeFieldDefinition(fieldName, bsonType) {
  if (bsonType === "int") {
    return {
      name: fieldName,
      bsonType,
      queryType: "range",
      min: new Int32(-2147483648),
      max: new Int32(2147483647),
      sparsity: 1,
      trimFactor: 6
    };
  }

  if (bsonType === "double") {
    return {
      name: fieldName,
      bsonType,
      queryType: "range",
      min: new Double(-1_000_000_000),
      max: new Double(1_000_000_000),
      precision: 2,
      sparsity: 1,
      trimFactor: 6
    };
  }

  return {
    name: fieldName,
    bsonType,
    queryType: "range",
    min: new Date("2000-01-01T00:00:00.000Z"),
    max: new Date("2100-01-01T00:00:00.000Z"),
    sparsity: 1,
    trimFactor: 6
  };
}

async function promptCustomFieldDefinitions(state) {
  const fieldBlueprints = [];

  while (true) {
    const { fieldName } = await inquirer.prompt([
      {
        type: "input",
        name: "fieldName",
        message: "Field name:",
        validate: (value) => {
          const trimmed = value.trim();
          if (!trimmed) {
            return "Field name cannot be empty.";
          }

          if (fieldBlueprints.some((field) => field.name === trimmed)) {
            return "Field name already added.";
          }

          return true;
        }
      }
    ]);

    const { bsonType } = await inquirer.prompt([
      {
        type: "list",
        name: "bsonType",
        message: "Field type:",
        choices: [
          { name: "string", value: "string" },
          { name: "int", value: "int" },
          { name: "double", value: "double" },
          { name: "bool", value: "bool" },
          { name: "date", value: "date" }
        ],
        default: "string"
      }
    ]);

    const { encrypted } = await inquirer.prompt([
      {
        type: "list",
        name: "encrypted",
        message: "Encrypt this field?",
        choices: [
          { name: "Yes", value: true },
          { name: "No", value: false }
        ],
        default: false
      }
    ]);

    let fieldDefinition = null;
    if (encrypted) {
      const { searchable } = await inquirer.prompt([
        {
          type: "list",
          name: "searchable",
          message: "Allow queries on this field?",
          choices: [
            { name: "No", value: false },
            { name: "Yes", value: true }
          ],
          default: false
        }
      ]);

      if (!searchable) {
        fieldDefinition = {
          name: fieldName.trim(),
          bsonType,
          queryType: null
        };
      } else {
        const queryChoices = [{ name: "Equality", value: "equality" }];
        if (supportsRangeForBsonType(bsonType) && supportsQueryableEncryptionRangeQueries(state.config.mongodbVersion)) {
          queryChoices.push({ name: "Range", value: "range" });
        }

        const { queryType } = await inquirer.prompt([
          {
            type: "list",
            name: "queryType",
            message: "Query capability:",
            choices: queryChoices,
            default: queryChoices[0].value
          }
        ]);

        if (queryType === "range") {
          fieldDefinition = buildRangeFieldDefinition(fieldName.trim(), bsonType);

          if (bsonType === "double") {
            const { precision } = await inquirer.prompt([
              {
                type: "input",
                name: "precision",
                message: "Decimal precision for range queries:",
                default: "2",
                validate: (value) => /^\d+$/.test(String(value).trim()) ? true : "Enter a non-negative integer."
              }
            ]);

            fieldDefinition.precision = Number(precision.trim());
          }
        } else {
          fieldDefinition = {
            name: fieldName.trim(),
            bsonType,
            queryType: "equality"
          };
        }
      }
    }

    fieldBlueprints.push({
      name: fieldName.trim(),
      bsonType,
      encrypted,
      fieldDefinition
    });

    const { nextAction } = await inquirer.prompt([
      {
        type: "list",
        name: "nextAction",
        message: "Do you want to add another field?",
        choices: [
          { name: "Add another field", value: "add" },
          { name: "Finish fields", value: "finish" }
        ],
        default: "add"
      }
    ]);

    if (nextAction === "finish") {
      return fieldBlueprints;
    }
  }
}

async function promptCustomDocuments(fieldBlueprints) {
  const documents = [];

  console.log("\nInsert 3 sample documents\n");

  for (let index = 0; index < 3; index += 1) {
    const document = {};
    console.log(`Document ${index + 1}`);

    for (const field of fieldBlueprints) {
      if (field.bsonType === "bool") {
        const { value } = await inquirer.prompt([
          {
            type: "list",
            name: "value",
            message: `${field.name} (${field.bsonType}):`,
            choices: [
              { name: "true", value: true },
              { name: "false", value: false }
            ],
            default: true
          }
        ]);
        document[field.name] = value;
        continue;
      }

      const { value } = await inquirer.prompt([
        {
          type: "input",
          name: "value",
          message: `${field.name} (${field.bsonType}):`,
          validate: (input) => validateInputForBsonType(input, field.bsonType)
        }
      ]);

      document[field.name] = parseInputForBsonType(value, field.bsonType);
    }

    documents.push(document);
    console.log("");
  }

  return documents;
}

function prepareDocumentsForEncryption(documents, fieldDefinitions) {
  return documents.map((document) => {
    const preparedDocument = { ...document };

    for (const fieldDefinition of fieldDefinitions) {
      if (Object.hasOwn(preparedDocument, fieldDefinition.name)) {
        preparedDocument[fieldDefinition.name] = coerceValueForField(preparedDocument[fieldDefinition.name], fieldDefinition);
      }
    }

    return preparedDocument;
  });
}

export function printQueryableEncryptionQuickstartPlan(state) {
  ensureQueryableEncryptionCompatible(state);

  const quickstartFieldNames = getAvailableDemoFields(state)
    .filter((field) => ["ssn", "email", "salary"].includes(field.name))
    .map((field) => field.name);
  const supportsRange = quickstartFieldNames.includes("salary");

  console.log("\nQueryable Encryption quickstart\n");
  console.log("This quickstart will:");
  console.log(`- create a local master key in ${getQeDirectory(state)}`);
  console.log(`- create the key vault collection ${getKeyVaultNamespace()}`);
  console.log(`- create ${DEFAULT_QE_DATABASE}.${DEFAULT_QE_COLLECTION}`);
  console.log(`- encrypt the fields ${quickstartFieldNames.join(", ")}`);
  console.log("- insert sample documents");
  console.log(supportsRange
    ? "- run equality and range queries against encrypted fields\n"
    : "- run an equality query against an encrypted field\n");
}

export function printQueryableEncryptionResources() {
  console.log("Queryable Encryption resources\n");
  for (const resource of QUERYABLE_ENCRYPTION_RESOURCES) {
    console.log(`- \x1b[1m${resource.label}\x1b[0m: ${resource.url}`);
  }
  console.log("");
}

async function runEqualityQuery(clients, options) {
  const {
    databaseName,
    collectionName,
    fieldName,
    value
  } = options;
  const decryptedDocument = await clients.qeClient
    .db(databaseName)
    .collection(collectionName)
    .findOne({ [fieldName]: value });

  if (!decryptedDocument) {
    return {
      decryptedDocument: null,
      rawStoredDocument: null
    };
  }

  const rawStoredDocument = await clients.regularClient
    .db(databaseName)
    .collection(collectionName)
    .findOne({ _id: decryptedDocument._id });

  return {
    decryptedDocument,
    rawStoredDocument
  };
}

async function runRangeQuery(clients, options) {
  const {
    databaseName,
    collectionName,
    fieldName,
    minValue,
    maxValue
  } = options;

  const decryptedDocuments = await clients.qeClient
    .db(databaseName)
    .collection(collectionName)
    .find({ [fieldName]: { $gte: minValue, $lte: maxValue } })
    .toArray();

  if (!decryptedDocuments.length) {
    return {
      decryptedDocuments: [],
      rawStoredDocuments: []
    };
  }

  const rawStoredDocuments = await clients.regularClient
    .db(databaseName)
    .collection(collectionName)
    .find({ _id: { $in: decryptedDocuments.map((document) => document._id) } })
    .toArray();

  return {
    decryptedDocuments,
    rawStoredDocuments
  };
}

function buildQueryExamples(selectedFields) {
  return buildQueryExamplesForDocuments(selectedFields, DEMO_DOCUMENTS);
}

function buildQueryExamplesForDocuments(selectedFields, documents) {
  const examples = [];
  const equalityField = selectedFields.find((field) => field.queryType === "equality");
  const rangeField = selectedFields.find((field) => field.queryType === "range");

  if (equalityField) {
    const equalityDocument = documents.find((document) => document[equalityField.name] !== undefined && document[equalityField.name] !== null);
    if (equalityDocument) {
      examples.push({
        mode: "equality",
        fieldName: equalityField.name,
        value: equalityDocument[equalityField.name]
      });
    }
  }

  if (rangeField) {
    const rangeValues = documents
      .map((document) => document[rangeField.name])
      .filter((value) => value !== undefined && value !== null)
      .sort((left, right) => {
        const leftValue = left instanceof Date ? left.getTime() : Number(left);
        const rightValue = right instanceof Date ? right.getTime() : Number(right);
        return leftValue - rightValue;
      });

    if (rangeValues.length) {
      examples.push({
        mode: "range",
        fieldName: rangeField.name,
        minValue: rangeValues[0],
        maxValue: rangeValues[Math.min(1, rangeValues.length - 1)]
      });
    }
  }

  return examples;
}
async function listUserCollections(state) {
  const clients = await createQeClients(state);
  const internalDatabases = new Set(["config", "local"]);

  try {
    const databases = await clients.regularClient.db("admin").admin().listDatabases();
    const collections = [];

    for (const database of databases.databases) {
      if (internalDatabases.has(database.name)) {
        continue;
      }

      const collectionInfos = await clients.regularClient
        .db(database.name)
        .listCollections({}, { nameOnly: true })
        .toArray();

      for (const collectionInfo of collectionInfos) {
        if (collectionInfo.name.startsWith("system.") || `${database.name}.${collectionInfo.name}` === getKeyVaultNamespace()) {
          continue;
        }

        const count = await clients.regularClient
          .db(database.name)
          .collection(collectionInfo.name)
          .countDocuments({});

        collections.push({
          databaseName: database.name,
          collectionName: collectionInfo.name,
          namespace: `${database.name}.${collectionInfo.name}`,
          count
        });
      }
    }

    return collections;
  } finally {
    await clients.regularClient.close();
    await clients.qeClient.close();
  }
}

async function listEncryptedCollections(state) {
  const clients = await createQeClients(state);
  const internalDatabases = new Set(["config", "local"]);

  try {
    const databases = await clients.regularClient.db("admin").admin().listDatabases();
    const collections = [];

    for (const database of databases.databases) {
      if (internalDatabases.has(database.name)) {
        continue;
      }

      const collectionInfos = await clients.regularClient
        .db(database.name)
        .listCollections({}, { nameOnly: false })
        .toArray();

      for (const collectionInfo of collectionInfos) {
        if (collectionInfo.name.startsWith("system.") || `${database.name}.${collectionInfo.name}` === getKeyVaultNamespace()) {
          continue;
        }

        const encryptedFieldDetails = parseEncryptedFieldDefinitions(
          collectionInfo.options?.encryptedFields ?? collectionInfo.encryptedFields ?? null
        );
        if (!encryptedFieldDetails.length) {
          continue;
        }

        const count = await clients.regularClient
          .db(database.name)
          .collection(collectionInfo.name)
          .countDocuments({});

        collections.push({
          databaseName: database.name,
          collectionName: collectionInfo.name,
          namespace: `${database.name}.${collectionInfo.name}`,
          count,
          encryptedFieldDetails,
          encryptedFieldsDocument: normalizeEncryptedFieldsDocument(
            collectionInfo.options?.encryptedFields ?? collectionInfo.encryptedFields
          )
        });
      }
    }

    return collections;
  } finally {
    await clients.regularClient.close();
    await clients.qeClient.close();
  }
}

async function renameCollection(client, databaseName, fromCollectionName, toCollectionName, options = {}) {
  await client
    .db(databaseName)
    .collection(fromCollectionName)
    .rename(toCollectionName, options);
}

async function saveQeState(state, qeState) {
  await ensureQeDirectory(state);
  await fs.writeFile(getQeStateFilePath(state), JSON.stringify(qeState, null, 2), "utf8");
}

async function loadQeState(state) {
  const filePath = getQeStateFilePath(state);
  if (!existsSync(filePath)) {
    return null;
  }

  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function confirmLocalKmsDemoUsage() {
  const { confirmed } = await inquirer.prompt([
    {
      type: "list",
      name: "confirmed",
      message: "This Queryable Encryption demo uses a local KMS key stored on disk and is not safe for production use. Do you want to continue?",
      choices: [
        { name: "I understand, continue", value: true },
        { name: "Back", value: false }
      ],
      default: false
    }
  ]);

  return confirmed;
}

export async function runQueryableEncryptionQuery(state) {
  ensureQueryableEncryptionCompatible(state);
  const collections = await listEncryptedCollections(state);

  if (!collections.length) {
    console.log("\nNo Queryable Encryption collections were found in this cluster yet.\n");
    return;
  }

  const { namespace } = await inquirer.prompt([
    {
      type: "list",
      name: "namespace",
      message: "Choose an encrypted collection:",
      choices: [
        ...collections.map((collection) => ({
          name: `${collection.namespace} (${collection.count} document${collection.count === 1 ? "" : "s"})`,
          value: collection.namespace
        })),
        { name: "Back", value: "back" }
      ],
      pageSize: 15
    }
  ]);

  if (namespace === "back") {
    return;
  }

  const selectedCollection = collections.find((collection) => collection.namespace === namespace);
  const queryableFields = selectedCollection.encryptedFieldDetails.filter((field) => field.queryType);

  if (!queryableFields.length) {
    console.log("\nThis collection has encrypted fields, but none of them allow queries.\n");
    return;
  }

  const { fieldName } = await inquirer.prompt([
    {
      type: "list",
      name: "fieldName",
      message: "Choose a queryable field:",
      choices: [
        ...queryableFields.map((field) => ({
          name: `${field.name} (${field.bsonType}, ${getQueryTypeLabel(field.queryType)})`,
          value: field.name
        })),
        { name: "Back", value: "back" }
      ]
    }
  ]);

  if (fieldName === "back") {
    return;
  }

  const selectedField = queryableFields.find((field) => field.name === fieldName);
  let mode = selectedField.queryType;

  if (selectedField.queryType === "range") {
    const { selectedMode } = await inquirer.prompt([
      {
        type: "list",
        name: "selectedMode",
        message: "Query mode:",
        choices: [
          { name: "Equality", value: "equality" },
          { name: "Range", value: "range" },
          { name: "Back", value: "back" }
        ],
        default: "equality"
      }
    ]);

    if (selectedMode === "back") {
      return;
    }

    mode = selectedMode;
  }

  const encryptedFieldsMap = {
    [`${selectedCollection.databaseName}.${selectedCollection.collectionName}`]: selectedCollection.encryptedFieldsDocument
  };
  const clients = await createQeClients(state, encryptedFieldsMap);

  try {
    if (mode === "equality") {
      const value = await promptQueryValue(
        `Value for ${selectedField.name} (${selectedField.bsonType}):`,
        selectedField.bsonType
      );
      const queryResult = await runEqualityQuery(clients, {
        databaseName: selectedCollection.databaseName,
        collectionName: selectedCollection.collectionName,
        fieldName: selectedField.name,
        value
      });

      console.log("\nQueryable Encryption query result\n");
      console.log(`Namespace: ${selectedCollection.namespace}`);
      console.log(`Field: ${selectedField.name}`);
      console.log(`Mode: equality`);
      console.log(`Value: ${JSON.stringify(summarizeValue(value))}`);
      console.log(`Found document: ${queryResult.decryptedDocument ? "yes" : "no"}\n`);

      if (queryResult.decryptedDocument) {
        console.log("Matching document\n");
        console.log(JSON.stringify(stripInternalQueryFields(summarizeValue(queryResult.decryptedDocument)), null, 2));
        console.log("");
      }
      return;
    }

    const minValue = await promptQueryValue(
      `Minimum value for ${selectedField.name} (${selectedField.bsonType}):`,
      selectedField.bsonType
    );
    const maxValue = await promptQueryValue(
      `Maximum value for ${selectedField.name} (${selectedField.bsonType}):`,
      selectedField.bsonType
    );
    const queryResult = await runRangeQuery(clients, {
      databaseName: selectedCollection.databaseName,
      collectionName: selectedCollection.collectionName,
      fieldName: selectedField.name,
      minValue,
      maxValue
    });

    console.log("\nQueryable Encryption query result\n");
    console.log(`Namespace: ${selectedCollection.namespace}`);
    console.log(`Field: ${selectedField.name}`);
    console.log(`Mode: range`);
    console.log(`Min: ${JSON.stringify(summarizeValue(minValue))}`);
    console.log(`Max: ${JSON.stringify(summarizeValue(maxValue))}`);
    console.log(`Matched documents: ${queryResult.decryptedDocuments.length}\n`);
    console.log("Matching documents\n");
    console.log(JSON.stringify(stripInternalQueryFields(summarizeValue(queryResult.decryptedDocuments)), null, 2));
    console.log("");
  } finally {
    await clients.regularClient.close();
    await clients.qeClient.close();
  }
}

function printQeSummary(summary) {
  console.log("\nQueryable Encryption setup completed\n");
  console.log(`Namespace: ${summary.databaseName}.${summary.collectionName}`);
  console.log(`Connection: ${summary.uri}`);
  console.log(`Key vault: ${summary.keyVaultNamespace}`);
  console.log(`Encrypted fields: ${summary.encryptedFields.join(", ")}`);
  console.log(`Documents inserted: ${summary.documentCount}\n`);
  console.log("Collection created. Connect to the cluster and inspect the stored documents to verify the encrypted values.\n");
}

async function runQeFlow(state, options) {
  ensureQueryableEncryptionCompatible(state);

  const {
    databaseName,
    collectionName,
    resetCollection
  } = options;

  const selectedFields = getFieldDefinitionsForFlow(state, options);
  if (!selectedFields.length) {
    throw new Error("Choose at least one field to encrypt.");
  }
  const sourceDocuments = options.documents ?? DEMO_DOCUMENTS;
  const preparedDocuments = prepareDocumentsForEncryption(sourceDocuments, selectedFields);

  const { clients, encryptedFields, backupCollectionName } = await createEncryptedCollection(state, {
    databaseName,
    collectionName,
    fieldDefinitions: selectedFields,
    resetCollection
  });

  try {
    await recreateQeClient(state, clients, {
      [`${databaseName}.${collectionName}`]: encryptedFields
    });

    await clients.qeClient
      .db(databaseName)
      .collection(collectionName)
      .insertMany(preparedDocuments, { ordered: true });

    const summary = {
      databaseName,
      collectionName,
      encryptedFields: selectedFields.map((field) => field.name),
      encryptedFieldDetails: selectedFields.map((field) => ({
        name: field.name,
        bsonType: field.bsonType,
        queryType: field.queryType
      })),
      keyVaultNamespace: clients.keyVaultNamespace,
      uri: clients.uri,
      documentCount: preparedDocuments.length
    };

    await saveQeState(state, summary);
    printQeSummary(summary);
    if (backupCollectionName) {
      console.log(`Backup collection preserved as ${databaseName}.${backupCollectionName}\n`);
    }
  } catch (error) {
    if (backupCollectionName) {
      try {
        await dropCollectionIfExists(clients.regularClient, databaseName, collectionName);
        await renameCollection(
          clients.regularClient,
          databaseName,
          backupCollectionName,
          collectionName
        );
      } catch {
        // Preserve the original error; restoration guidance is handled in the thrown message.
      }

      throw new Error(`Queryable Encryption setup failed without deleting the original collection. ${error.message}`);
    }

    throw error;
  } finally {
    await clients.regularClient.close();
    await clients.qeClient.close();
  }
}

export async function runQueryableEncryptionQuickstart(state) {
  if (!await confirmLocalKmsDemoUsage()) {
    console.log("\nQueryable Encryption quickstart cancelled.\n");
    return;
  }

  const quickstartFieldNames = getAvailableDemoFields(state)
    .filter((field) => ["ssn", "email", "salary"].includes(field.name))
    .map((field) => field.name);

  await runQeFlow(state, {
    databaseName: DEFAULT_QE_DATABASE,
    collectionName: DEFAULT_QE_COLLECTION,
    encryptedFieldNames: quickstartFieldNames,
    resetCollection: true
  });
}

export async function runQueryableEncryptionSetup(state) {
  ensureQueryableEncryptionCompatible(state);
  let databaseName = DEFAULT_QE_DATABASE;
  let collectionName = DEFAULT_QE_COLLECTION;

  if (!await confirmLocalKmsDemoUsage()) {
    console.log("\nQueryable Encryption setup cancelled.\n");
    return;
  }

  const { action } = await inquirer.prompt([
    {
      type: "list",
      name: "action",
      message: "Create custom encrypted collection",
      choices: [
        { name: "Continue", value: "continue" },
        { name: "Back", value: "back" }
      ],
      default: "continue"
    }
  ]);

  if (action === "back") {
    return;
  }

  while (true) {
    const { databaseAction } = await inquirer.prompt([
      {
        type: "list",
        name: "databaseAction",
        message: `Database name: ${databaseName}`,
        choices: [
          { name: "Edit database name", value: "edit" },
          { name: "Back", value: "back" }
        ],
        default: "edit"
      }
    ]);

    if (databaseAction === "back") {
      return;
    }

    const databaseAnswer = await inquirer.prompt([
      {
        type: "input",
        name: "databaseName",
        message: "Database name:",
        default: databaseName,
        validate: (value) => (value.trim() ? true : "Database name cannot be empty.")
      }
    ]);
    databaseName = databaseAnswer.databaseName.trim();

    while (true) {
      const { collectionAction } = await inquirer.prompt([
        {
          type: "list",
          name: "collectionAction",
          message: `Collection name: ${collectionName}`,
          choices: [
            { name: "Edit collection name", value: "edit" },
            { name: "Back", value: "back" }
          ],
          default: "edit"
        }
      ]);

      if (collectionAction === "back") {
        break;
      }

      const collectionAnswer = await inquirer.prompt([
        {
          type: "input",
          name: "collectionName",
          message: "Collection name:",
          default: collectionName,
          validate: (value) => (value.trim() ? true : "Collection name cannot be empty.")
        }
      ]);
      collectionName = collectionAnswer.collectionName.trim();

      const fieldBlueprints = await promptCustomFieldDefinitions(state);
      const fieldDefinitions = fieldBlueprints
        .filter((field) => field.encrypted)
        .map((field) => field.fieldDefinition);

      if (!fieldDefinitions.length) {
        console.log("\nAdd at least one encrypted field to create a Queryable Encryption collection.\n");
        continue;
      }

      const documents = await promptCustomDocuments(fieldBlueprints);
      console.log("Sample documents\n");
      console.log(JSON.stringify(documents.map((document) => summarizeValue(document)), null, 2));
      console.log("");

      const { confirmed } = await inquirer.prompt([
        {
          type: "list",
          name: "confirmed",
          message: "Create this encrypted collection with the 3 sample documents?",
          choices: [
            { name: "Create collection", value: true },
            { name: "Back", value: false }
          ],
          default: true
        }
      ]);

      if (!confirmed) {
        continue;
      }

      await runQeFlow(state, {
        databaseName,
        collectionName,
        fieldDefinitions,
        documents,
        resetCollection: false
      });
      return;
    }
  }
}

export async function runQueryableEncryptionStatus(state) {
  ensureQueryableEncryptionCompatible(state);
  const qeState = await loadQeState(state);

  if (!qeState) {
    console.log("\nNo Queryable Encryption lab state found for this cluster yet.\n");
    return;
  }

  const storedFieldDetails = qeState.encryptedFieldDetails
    ?? qeState.encryptedFields.map((fieldName) => getFieldQuerySummary(fieldName)).filter(Boolean);
  const clients = await createQeClients(state, {
    [`${qeState.databaseName}.${qeState.collectionName}`]: buildEncryptedFieldsDocument(storedFieldDetails)
  });

  try {
    const collectionInfo = await clients.regularClient
      .db(qeState.databaseName)
      .listCollections({ name: qeState.collectionName }, { nameOnly: false })
      .toArray();
    const documentCount = await clients.regularClient
      .db(qeState.databaseName)
      .collection(qeState.collectionName)
      .countDocuments({});
    const keyCount = await clients.regularClient
      .db(KEY_VAULT_DATABASE)
      .collection(KEY_VAULT_COLLECTION)
      .countDocuments({});

    console.log("\nQueryable Encryption status\n");
    console.log(`Namespace: ${qeState.databaseName}.${qeState.collectionName}`);
    console.log(`Connection: ${qeState.uri}`);
    console.log(`Key vault: ${qeState.keyVaultNamespace}`);
    console.log(`Encrypted fields: ${storedFieldDetails.map((field) => `${field.name} (${getQueryTypeLabel(field.queryType)})`).join(", ")}`);
    console.log(`Documents: ${documentCount}`);
    console.log(`Keys in vault: ${keyCount}`);
    console.log(`Collection exists: ${collectionInfo.length ? "yes" : "no"}\n`);
  } finally {
    await clients.regularClient.close();
    await clients.qeClient.close();
  }
}

export async function interactiveQueryableEncryptionMenu(state) {
  ensureQueryableEncryptionCompatible(state);

  let exitMenu = false;
  while (!exitMenu) {
    const { action } = await inquirer.prompt([
      {
        type: "list",
        name: "action",
        message: "Queryable Encryption\nUse explicit client-side encryption with local KMS",
        choices: [
          { name: "1. Run quickstart demo", value: "quickstart" },
          { name: "2. Create custom encrypted collection", value: "setup" },
          { name: "3. Query encrypted collection", value: "query" },
          { name: "4. Show Queryable Encryption status", value: "status" },
          { name: "5. Back", value: "back" }
        ],
        default: "quickstart"
      }
    ]);

    if (action === "quickstart") {
      await runQueryableEncryptionQuickstart(state);
      continue;
    }

    if (action === "setup") {
      await runQueryableEncryptionSetup(state);
      continue;
    }

    if (action === "query") {
      await runQueryableEncryptionQuery(state);
      continue;
    }

    if (action === "status") {
      await runQueryableEncryptionStatus(state);
      continue;
    }

    exitMenu = true;
  }
}
