import * as fs from 'fs';
import path from 'path';

import { DatabaseConnector, DatabaseConnectorBuilder } from '../../../src/database/database-connector-builder';
import { SchemaAnalyseClass } from '../../../src/schema/schema-analyse.class';


const mockFolder = './test/e2e/schema-analyse/mocks';

describe('Schema analyse class', () => {
   describe(`generateSchemaFromDB`, () => {
     let schemaAnalyse: SchemaAnalyseClass;
     beforeEach(() => {
       schemaAnalyse = new SchemaAnalyseClass('schema');
     });

     describe('Engine MySql and database type maria-db', () => {
       let dbConnector: DatabaseConnector;

       afterEach(() => {
         jest.resetAllMocks();
       });

       afterAll(async () => {
         await dbConnector.destroy();
       });

       describe('When i run generation of schema without any db connector ', () => {
         it('it should throw exception with db connector not found message', async () => {
           await expect(
             async () => schemaAnalyse.generateSchemaFromDB(undefined),
           ).rejects.toThrow(new Error('DB connection not ready'))
         });
       });

       describe('When i run generation of schema with db connector mariadb ', () => {
         it('it should return all tables and definitions for current database', async () => {
           // given
           jest.spyOn(schemaAnalyse as any, 'storeSchemaToDisk').mockImplementation(() => null);
           const expectedJson = JSON.parse(
             fs.readFileSync(
               path.resolve(process.cwd(), `${mockFolder}/maria-db.schema.json`),
               'utf8',
             ),
           );

           const uriScheme = 'mysql';
           const dbSchema = 'maria-db';
           const dbUri = 'mysql://root:maria-db@127.0.0.1:3306/maria-db';
           dbConnector = await (new DatabaseConnectorBuilder(dbUri, dbSchema)).build(uriScheme);

           // when
           const schema = await schemaAnalyse.generateSchemaFromDB(dbConnector);

           // then
           expect(schema.toJSON()).toEqual(expectedJson);
         });
       });
     });
   });
});
