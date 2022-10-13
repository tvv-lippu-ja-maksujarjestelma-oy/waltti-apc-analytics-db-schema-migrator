import fs from "fs";
import type pg from "pg";

export interface HealthCheckConfig {
  port: number;
}

export interface Config {
  client: pg.ClientConfig;
  migrationsPath: string;
  healthCheck: HealthCheckConfig;
}

const getRequired = (envVariable: string) => {
  const variable = process.env[envVariable];
  if (variable === undefined) {
    throw new Error(`${envVariable} must be defined`);
  }
  return variable;
};

const getOptional = (envVariable: string) => process.env[envVariable];

const getOptionalBooleanWithDefault = (
  envVariable: string,
  defaultValue: boolean
) => {
  let result = defaultValue;
  const str = getOptional(envVariable);
  if (str !== undefined) {
    if (!["false", "true"].includes(str)) {
      throw new Error(`${envVariable} must be either "false" or "true"`);
    }
    result = str === "true";
  }
  return result;
};

const getClientConfig = () => {
  const database = getOptional("POSTGRES_DB") ?? "postgres";
  const user = fs.readFileSync(getRequired("POSTGRES_USER_PATH"), "utf8");
  const password = fs.readFileSync(
    getRequired("POSTGRES_PASSWORD_PATH"),
    "utf8"
  );
  const host = getRequired("POSTGRES_HOST");
  const port = parseInt(getOptional("POSTGRES_PORT") ?? "5432", 10);
  const ssl = getOptionalBooleanWithDefault("POSTGRES_USE_SSL", true);
  return {
    database,
    user,
    password,
    host,
    port,
    ssl,
  };
};

const getMigrationsPath = () => {
  const migrationsPath = getRequired("MIGRATIONS_PATH");
  if (!fs.statSync(migrationsPath).isDirectory()) {
    throw new Error(`${migrationsPath} must be an existing directory`);
  }
  return migrationsPath;
};

const getHealthCheckConfig = () => {
  const port = parseInt(getOptional("HEALTH_CHECK_PORT") ?? "8080", 10);
  return { port };
};

export const getConfig = (): Config => ({
  client: getClientConfig(),
  migrationsPath: getMigrationsPath(),
  healthCheck: getHealthCheckConfig(),
});
