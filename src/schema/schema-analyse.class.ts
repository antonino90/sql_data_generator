import * as fs from 'fs-extra';
import { getLogger } from 'log4js';
import * as path from 'path';
import 'reflect-metadata';

import { DatabaseConnector } from '../database/database-connector-builder';
import { CustomSchema } from './custom-schema.class';
import { Schema } from './schema.validator';

const logger = getLogger();
logger.level = 'debug';

export class SchemaAnalyseClass {
    private readonly schema: string;

    constructor(schema: string = 'schema') {
        this.schema = schema;
    }

    public async generateSchemaFromDB(dbConnector: DatabaseConnector | undefined): Promise<Schema> {
        if (!dbConnector) throw new Error('DB connection not ready');
        const schema = await SchemaAnalyseClass.generateSchema(dbConnector);
        this.storeSchemaToDisk(schema);
        return schema;
    }

    private static async generateSchema(dbConnector: DatabaseConnector) {
        return await dbConnector.getSchema();
    }

    private storeSchemaToDisk(schema: Schema) {
        fs.writeJSONSync(path.join('settings', `${this.schema}.json`), schema.toJSON(), {spaces: 4});
        if (!fs.existsSync(path.join('settings', `${this.schema}_custom.jsonc`))) {
            const customSchema = new CustomSchema();
            fs.writeJSONSync(path.join('settings', `${this.schema}_custom.jsonc`), customSchema, {spaces: 4});
        }
    }
}