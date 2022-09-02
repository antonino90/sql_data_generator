import * as fs from 'fs';
import path from 'path';

import { DatabaseConnector, DatabaseConnectorBuilder } from '../../../src/database/database-connector-builder';
import { SchemaAnalyseClass } from '../../../src/schema/schema-analyse.class';

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

describe('Schema analyse class', () => {
  let dbConnector: DatabaseConnector;

  afterEach(async () => {
    jest.clearAllMocks();
    if (dbConnector) {
      await dbConnector.destroy();
    }
  });

  describe(`generateSchemaFromDB`, () => {
    let schemaAnalyse: SchemaAnalyseClass;
    beforeEach(() => {
      schemaAnalyse = new SchemaAnalyseClass('schema');
    });

    describe('When i run generation of schema without any db connector ', () => {
      it('it should throw exception with db connector not found message', async () => {
        await expect(
          async () => schemaAnalyse.generateSchemaFromDB(undefined),
        ).rejects.toThrow(new Error('DB connection not ready'))
      });
      expect(loggerInstance.warn).toHaveBeenCalledTimes(0)
      expect(loggerInstance.info).toHaveBeenCalledTimes(0);
    });

    describe('Engine "mysql" and database type maria-db', () => {
      describe('When i run generation of schema with db connector mariadb ', () => {
        it('it should return all tables and definitions for current database', async () => {
          // given
          const expectedWarnLog = [
            'For performance foreign_key_checks, autocommit and unique_checks are disabled during insert.',
            'They are disabled per connections and should not alter your configuration.',
          ];
          const expectedInfoLog = [
            'To improve performances further you can update innodb_autoinc_lock_mode = 0 in your my.ini.',
            'test_types',
          ];
          const expectedJson = JSON.parse(
            fs.readFileSync(
              path.resolve(process.cwd(), `${mockFolder}/maria-db.schema.json`),
              'utf8',
            ),
          );
          const storeSchemaToDiskMock = jest.spyOn(schemaAnalyse as any, 'storeSchemaToDisk').mockImplementation(() => null);

          const uriScheme = 'mysql';
          const dbSchema = 'maria-db';
          const dbUri = 'mysql://root:maria-db@127.0.0.1:3306/maria-db';
          dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

          // when
          const schema = await schemaAnalyse.generateSchemaFromDB(dbConnector);

          // then
          expect(schema.toJSON()).toEqual(expectedJson);
          expect(storeSchemaToDiskMock).toHaveBeenCalledTimes(1);

          expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
          expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));

          expect(loggerInstance.info).toHaveBeenCalledTimes(expectedInfoLog.length);
          expectedInfoLog.forEach((value, index) => expect(loggerInstance.info).toHaveBeenNthCalledWith(index + 1, value));
        });
      });
    });

    describe('Engine “postgres" and database type postgreSQL', () => {
      describe('When i run generation of schema with db connector postgres ', () => {
        it('it should return all tables and definitions for current database', async () => {
          // given
          const expectedWarnLog = [
            'For performance session_replication_role, enable_seqscan are disabled during insert.',
            'They are disabled per connections and should not alter your configuration.',
          ];
          const expectedInfoLog = [
            'test_types',
          ];
          const expectedJson = JSON.parse(
            fs.readFileSync(
              path.resolve(process.cwd(), `${mockFolder}/pgsql-db.schema.json`),
              'utf8',
            ),
          );
          const storeSchemaToDiskMock = jest.spyOn(schemaAnalyse as any, 'storeSchemaToDisk').mockImplementation(() => null);

          const uriScheme = 'postgres';
          const dbSchema = 'pgsql-db';
          const dbUri = 'postgres://pgsql-db:pgsql-db@127.0.0.1:5432/pgsql-db';
          dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

          // when
          const schema = await schemaAnalyse.generateSchemaFromDB(dbConnector);

          // then
          expect(schema.toJSON()).toEqual(expectedJson);
          expect(storeSchemaToDiskMock).toHaveBeenCalledTimes(1);

          expect(loggerInstance.warn).toHaveBeenCalledTimes(expectedWarnLog.length);
          expectedWarnLog.forEach((value, index) => expect(loggerInstance.warn).toHaveBeenNthCalledWith(index + 1, value));

          expect(loggerInstance.info).toHaveBeenCalledTimes(expectedInfoLog.length);
          expectedInfoLog.forEach((value, index) => expect(loggerInstance.info).toHaveBeenNthCalledWith(index + 1, value));
        });
      });
    });
  });
});
