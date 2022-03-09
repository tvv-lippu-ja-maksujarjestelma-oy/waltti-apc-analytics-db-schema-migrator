import moment from "moment";
import pg from "pg";
import pino from "pino";
import * as postgresMigrations from "postgres-migrations";
import { getConfig } from "./config";
import createHealthCheckServer from "./healthCheck";
import transformUnknownToError from "./util";

/**
 * Exit gracefully.
 */
const exitGracefully = async (
  logger: pino.Logger,
  exitCode: number,
  exitError?: Error,
  setHealthOk?: (isOk: boolean) => void,
  closeHealthCheckServer?: () => Promise<void>,
  client?: pg.Client
) => {
  if (exitError) {
    logger.fatal(exitError);
  }
  logger.info("Start exiting gracefully");
  process.exitCode = exitCode;
  try {
    if (setHealthOk) {
      logger.info("Set health checks to fail");
      setHealthOk(false);
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when setting health checks to fail"
    );
  }
  try {
    if (client) {
      logger.info("Close database client");
      await client.end();
    }
  } catch (err) {
    logger.error({ err }, "Something went wrong when closing database client");
  }
  try {
    if (closeHealthCheckServer) {
      logger.info("Close health check server");
      await closeHealthCheckServer();
    }
  } catch (err) {
    logger.error(
      { err },
      "Something went wrong when closing health check server"
    );
  }
  logger.info("Exit process");
  process.exit(); // eslint-disable-line no-process-exit
};

/**
 * Main function.
 */
/* eslint-disable @typescript-eslint/no-floating-promises */
(async () => {
  /* eslint-enable @typescript-eslint/no-floating-promises */
  try {
    const logger = pino({
      name: "waltti-apc-analytics-db-schema-migrator",
      timestamp: pino.stdTimeFunctions.isoTime,
    });

    let setHealthOk: (isOk: boolean) => void;
    let closeHealthCheckServer: () => Promise<void>;
    let client: pg.Client;

    const exitHandler = (exitCode: number, exitError?: Error) => {
      // Exit next.
      /* eslint-disable @typescript-eslint/no-floating-promises */
      exitGracefully(
        logger,
        exitCode,
        exitError,
        setHealthOk,
        closeHealthCheckServer,
        client
      );
      /* eslint-enable @typescript-eslint/no-floating-promises */
    };

    try {
      // Handle different kinds of exits.
      process.on("beforeExit", () => exitHandler(0));
      process.on("unhandledRejection", (reason) =>
        exitHandler(1, transformUnknownToError(reason))
      );
      process.on("uncaughtException", (err) => exitHandler(1, err));
      process.on("SIGINT", (signal) => exitHandler(130, new Error(signal)));
      process.on("SIGQUIT", (signal) => exitHandler(131, new Error(signal)));
      process.on("SIGTERM", (signal) => exitHandler(143, new Error(signal)));

      logger.info("Read configuration");
      const config = getConfig();
      logger.info("Create health check server");
      ({ closeHealthCheckServer, setHealthOk } = createHealthCheckServer(
        config.healthCheck
      ));

      // Correct pg date handling:
      // https://github.com/ThomWright/postgres-migrations/blob/dbfc5ccd7c71d77c24200d403cd72722017eca67/README.md#date-handling
      const parseDate = (val: string) =>
        val === null ? null : moment(val).format("YYYY-MM-DD");
      const DATATYPE_DATE = 1082;
      pg.types.setTypeParser(DATATYPE_DATE, (val) =>
        val === null ? null : parseDate(val)
      );

      logger.info("Connect to database");
      client = new pg.Client(config.client);
      await client.connect();
      logger.info("Set health check status to OK");
      setHealthOk(true);
      const migrationLogger = (message: string) =>
        logger.info({ messageSource: "postgres-migrations" }, message);
      logger.info("Start SQL schema migrations");
      await postgresMigrations.migrate({ client }, config.migrationsPath, {
        logger: migrationLogger,
      });
      exitHandler(0);
    } catch (err) {
      exitHandler(1, transformUnknownToError(err));
    }
  } catch (loggerErr) {
    // eslint-disable-next-line no-console
    console.error("Failed to start logging:", loggerErr);
    process.exit(1); // eslint-disable-line no-process-exit
  }
})();
