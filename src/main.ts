import { CliMain, CliMainClass, CliParameter } from '@corteks/clify';
import * as fs from 'fs-extra';
import { getLogger } from 'log4js';
import * as path from 'path';
import 'reflect-metadata';
import * as URI from 'uri-js';

import { DatabaseConnector, DatabaseConnectorBuilder } from './database/database-connector-builder';
import { SchemaAnalyseClass } from './schema/schema-analyse.class';
import { DataGeneratorClass } from './generation/data-generator.class';

const logger = getLogger();
logger.level = 'debug';

@CliMain
class Main extends CliMainClass {
    @CliParameter({ alias: 'db', demandOption: true, description: 'Database URI. Eg: mysql://user:password@127.0.0.1:3306/database' })
    private uri: string | undefined = undefined;

    @CliParameter({ description: 'Extra schema information and generate default settings' })
    private analyse: boolean = false;

    @CliParameter({ description: 'Empty tables before filling them' })
    private reset: boolean = false;

    @CliParameter({ description: 'Schema filename to use. Will be generated with --analyse' })
    private schema: string = 'schema';

    @CliParameter({ description: 'Database schema to use. Database name will be used by default as database schema value' })
    private dbSchema: string = '';

    private dbConnector: DatabaseConnector | undefined;

    async main(): Promise<number> {
        if (!this.uri) {
            throw new Error('Please provide a valid database uri')
        }

        try {
            const { uriScheme, dbSchema } = Main.extractDataFromCliParameters(this.uri, this.dbSchema);
            this.dbConnector = await (new DatabaseConnectorBuilder(this.uri, dbSchema)).build(uriScheme);
        } catch (err) {
            logger.error((err as Error).message);
            return 1;
        }

        try {
            if (this.analyse) {
                await (new SchemaAnalyseClass(this.schema)).generateSchemaFromDB(this.dbConnector);
                return 0;
            }

            await (new DataGeneratorClass(this.schema, this.uri)).generateDataInDB(this.dbConnector, this.reset);
        } catch (ex) {
            if ((ex as any).code === 'ENOENT') {
                logger.error(`Unable to read from ./settings/${this.schema}.json. Please run with --analyse first.`);
            } else {
                logger.error(ex);
            }
        } finally {
            logger.info('Close database connection');
            await this.dbConnector.destroy();
        }
        return 0;
    }

    private static extractDataFromCliParameters(uri: string, dbSchema: string | undefined) {
        const uriComponents = URI.parse(uri);
        if (!uriComponents.path) {
            throw new Error('Please specify database name')
        }

        return {
            uriScheme: uriComponents.scheme || '',
            dbSchema: (dbSchema) ? dbSchema : uriComponents.path.replace('/', ''),
        };
    }
}