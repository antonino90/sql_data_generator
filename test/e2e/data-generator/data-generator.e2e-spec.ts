import * as fs from 'fs';
import path from 'path';

import { DatabaseConnector, DatabaseConnectorBuilder } from '../../../src/database/database-connector-builder';
import { DataGeneratorClass } from '../../../src/generation/data-generator.class';

const loggerInstance = {
  level: jest.fn(),
  warn: jest.fn(),
  info: jest.fn(),
  error: jest.fn(),
  log: jest.fn(),
}

jest.mock('log4js', () => ({
  getLogger: jest.fn().mockImplementation((value) => (loggerInstance)),
}));

const mockFolder = './test/e2e/mocks';

describe('Data generator class', () => {
  let dbConnector: DatabaseConnector;

  afterEach(async () => {
    jest.clearAllMocks();
    if (dbConnector) {
      await dbConnector.destroy();
    }
  });

  describe('engine "mariadb" -> generateDataInDB', () => {
    let dataGenerator: DataGeneratorClass;
    let dbUri: string;

    beforeEach(() => {
      dbUri = 'mysql://root:maria-db@127.0.0.1:3306/maria-db';
      dataGenerator = new DataGeneratorClass('schema', dbUri);
    });

    describe('When i run generation of schema without any db connector ', () => {
      it('it should throw exception with db connector not found message', async () => {
        await expect(
          async () => dataGenerator.generateDataInDB(undefined),
        ).rejects.toThrow(new Error('DB connection not ready'))
      });
      expect(loggerInstance.warn).toHaveBeenCalledTimes(0)
      expect(loggerInstance.info).toHaveBeenCalledTimes(0);
    });
    describe('When i run generation of data in DB WITHOUT reset of existing data', () => {
      it('it should generate data in database related to schema json', async () => {
        // given
        const uriScheme = 'mysql';
        const dbSchema = 'maria-db';
        dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

        const expectedWarnLog = [
          'For performance foreign_key_checks, autocommit and unique_checks are disabled during insert.',
          'They are disabled per connections and should not alter your configuration.',
          `Unable to read ./settings/schema_custom.json, this will not take any customization into account.`,
        ];
        const expectedJson = JSON.parse(
          fs.readFileSync(
            path.resolve(process.cwd(), `${mockFolder}/maria-db.schema.json`),
            'utf8',
          ),
        );
        const getSchemaMock = jest.spyOn(dataGenerator as any, 'getSchema').mockReturnValue(expectedJson);
        const getCustomSqlScriptsMock = jest.spyOn(DataGeneratorClass as any, 'getCustomSqlScripts').mockReturnValue([]);
        const backupTriggersMock = jest.spyOn(dbConnector as any, 'backupTriggers').mockReturnValue(null);
        const cleanBackupTriggersMock = jest.spyOn(dbConnector as any, 'cleanBackupTriggers').mockReturnValue(null);
        const withResetOfExistingDatabaseValues = false;

        // when
        await dataGenerator.generateDataInDB(dbConnector, withResetOfExistingDatabaseValues);

        // then
        expect(getSchemaMock).toHaveBeenCalledTimes(1);
        expect(getCustomSqlScriptsMock).toHaveBeenCalledTimes(1);
        expect(backupTriggersMock).toHaveBeenCalledTimes(1);
        expect(cleanBackupTriggersMock).toHaveBeenCalledTimes(1);
        expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
        expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));
      });
    });
    describe('When i run generation of data in DB WITH reset of existing data', () => {
      it('it should replace existing data with new generated data related to schema json', async () => {
        // given
        const uriScheme = 'mysql';
        const dbSchema = 'maria-db';
        dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

        const expectedWarnLog = [
          'For performance foreign_key_checks, autocommit and unique_checks are disabled during insert.',
          'They are disabled per connections and should not alter your configuration.',
          `Unable to read ./settings/schema_custom.json, this will not take any customization into account.`,
        ];
        const expectedJson = JSON.parse(
          fs.readFileSync(
            path.resolve(process.cwd(), `${mockFolder}/maria-db.schema.json`),
            'utf8',
          ),
        );
        const getSchemaMock = jest.spyOn(dataGenerator as any, 'getSchema').mockReturnValue(expectedJson);
        const getCustomSqlScriptsMock = jest.spyOn(DataGeneratorClass as any, 'getCustomSqlScripts').mockReturnValue([]);
        const backupTriggersMock = jest.spyOn(dbConnector as any, 'backupTriggers').mockReturnValue(null);
        const cleanBackupTriggersMock = jest.spyOn(dbConnector as any, 'cleanBackupTriggers').mockReturnValue(null);
        const withResetOfExistingDatabaseValues = true;

        // when
        await dataGenerator.generateDataInDB(dbConnector, withResetOfExistingDatabaseValues);

        // then
        expect(getSchemaMock).toHaveBeenCalledTimes(1);
        expect(getCustomSqlScriptsMock).toHaveBeenCalledTimes(1);
        expect(backupTriggersMock).toHaveBeenCalledTimes(1);
        expect(cleanBackupTriggersMock).toHaveBeenCalledTimes(1);
        expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
        expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));
      });
    });
  });

  describe('engine "postgres" -> generateDataInDB', () => {
    let dataGenerator: DataGeneratorClass;
    let dbUri: string;

    beforeEach(() => {
      dbUri = 'postgres://pgsql-db:pgsql-db@127.0.0.1:5432/pgsql-db';
      dataGenerator = new DataGeneratorClass('schema', dbUri);
    });

    describe('When i run generation of schema without any db connector ', () => {
      it('it should throw exception with db connector not found message', async () => {
        await expect(
          async () => dataGenerator.generateDataInDB(undefined),
        ).rejects.toThrow(new Error('DB connection not ready'))
      });
      expect(loggerInstance.warn).toHaveBeenCalledTimes(0)
      expect(loggerInstance.info).toHaveBeenCalledTimes(0);
    });
    describe('When i run generation of data in DB WITHOUT reset of existing data', () => {
      it('it should generate data in database related to schema json', async () => {
        // given
        const uriScheme = 'postgres';
        const dbSchema = 'pgsql-db';
        dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

        const expectedWarnLog = [
          'For performance session_replication_role, enable_seqscan are disabled during insert.',
          'They are disabled per connections and should not alter your configuration.',
          `Unable to read ./settings/schema_custom.json, this will not take any customization into account.`,
        ];
        const expectedJson = JSON.parse(
          fs.readFileSync(
            path.resolve(process.cwd(), `${mockFolder}/pgsql-db.schema.json`),
            'utf8',
          ),
        );
        const getSchemaMock = jest.spyOn(dataGenerator as any, 'getSchema').mockReturnValue(expectedJson);
        const getCustomSqlScriptsMock = jest.spyOn(DataGeneratorClass as any, 'getCustomSqlScripts').mockReturnValue([]);
        const backupTriggersMock = jest.spyOn(dbConnector as any, 'backupTriggers').mockReturnValue(null);
        const cleanBackupTriggersMock = jest.spyOn(dbConnector as any, 'cleanBackupTriggers').mockReturnValue(null);
        const withResetOfExistingDatabaseValues = false;

        // when
        await dataGenerator.generateDataInDB(dbConnector, withResetOfExistingDatabaseValues);

        // then
        expect(getSchemaMock).toHaveBeenCalledTimes(1);
        expect(getCustomSqlScriptsMock).toHaveBeenCalledTimes(1);
        expect(backupTriggersMock).toHaveBeenCalledTimes(1);
        expect(cleanBackupTriggersMock).toHaveBeenCalledTimes(1);
        expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
        expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));
      });
    });
    describe('When i run generation of data in DB WITH reset of existing data', () => {
      it('it should replace existing data with new generated data related to schema json', async () => {
        // given
        const uriScheme = 'postgres';
        const dbSchema = 'pgsql-db';
        dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

        const expectedWarnLog = [
          'For performance session_replication_role, enable_seqscan are disabled during insert.',
          'They are disabled per connections and should not alter your configuration.',
          `Unable to read ./settings/schema_custom.json, this will not take any customization into account.`,
        ];
        const expectedJson = JSON.parse(
          fs.readFileSync(
            path.resolve(process.cwd(), `${mockFolder}/pgsql-db.schema.json`),
            'utf8',
          ),
        );
        const getSchemaMock = jest.spyOn(dataGenerator as any, 'getSchema').mockReturnValue(expectedJson);
        const getCustomSqlScriptsMock = jest.spyOn(DataGeneratorClass as any, 'getCustomSqlScripts').mockReturnValue([]);
        const backupTriggersMock = jest.spyOn(dbConnector as any, 'backupTriggers').mockReturnValue(null);
        const cleanBackupTriggersMock = jest.spyOn(dbConnector as any, 'cleanBackupTriggers').mockReturnValue(null);
        const withResetOfExistingDatabaseValues = true;

        // when
        await dataGenerator.generateDataInDB(dbConnector, withResetOfExistingDatabaseValues);

        // then
        expect(getSchemaMock).toHaveBeenCalledTimes(1);
        expect(getCustomSqlScriptsMock).toHaveBeenCalledTimes(1);
        expect(backupTriggersMock).toHaveBeenCalledTimes(1);
        expect(cleanBackupTriggersMock).toHaveBeenCalledTimes(1);
        expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
        expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));
      });
    });
  });
});
