import { CliMain, CliMainClass, CliParameter } from '@corteks/clify';
import * as fs from 'fs-extra';
import { getLogger } from 'log4js';
import * as path from 'path';
import 'reflect-metadata';

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

    @CliParameter({ description: 'Database schema to use. Schema with value "public" will be used by default' })
    private dbSchema: string = 'public';

    private dbConnector: DatabaseConnector | undefined;

    async main(): Promise<number> {
        if (!this.uri) throw new Error('Please provide a valid database uri');

        const dbConnectorBuilder = new DatabaseConnectorBuilder(this.uri, this.dbSchema);
        try {
            this.dbConnector = await dbConnectorBuilder.build();
        } catch (err) {
            logger.error((err as Error).message);
            return 1;
        }
        if (!fs.pathExistsSync('settings')) {
            fs.mkdirSync('settings');
        }
        if (!fs.pathExistsSync(path.join('settings', 'scripts'))) {
            fs.mkdirSync(path.join('settings', 'scripts'));
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
}