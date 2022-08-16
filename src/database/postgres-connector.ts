import * as fs from 'fs-extra';
import Knex from 'knex';
import { getLogger } from 'log4js';
import { Connection, MysqlError } from 'mysql';
import * as path from 'path';
import * as URI from 'uri-js';
import { Generators } from '../generation/generators/generators';
import { Column, Schema, Table } from '../schema/schema.class';
import { DatabaseConnector } from './database-connector-builder';

export class PostgresConnector implements DatabaseConnector {
    private dbConnection: Knex;
    private triggers: Trigger[] = [];
    private logger = getLogger();
    private triggerBackupFile: string = path.join('settings', 'triggers.json');
    private uriComponents: URI.URIComponents;
    private database: string;

    constructor(
        private uri: string,
    ) {
        this.uriComponents = URI.parse(this.uri);
        if (!this.uriComponents.path) throw new Error('Please specify database name');

        //this.database = this.uriComponents.path.replace('/', ''); // todo add cli parameter for that
        this.database = 'public';

        this.dbConnection = Knex({
            client: 'pg',
            // wrapIdentifier: (value, origImpl, queryContext) => value, to remove the quotes
            connection: this.uri,
            log: {
                warn: (message) => {
                    this.logger.warn(message);
                },
                error: (message) => {
                    this.logger.error(message);
                },
                deprecate: (message) => {
                    this.logger.warn(message);
                },
                debug: (message) => {
                    this.logger.debug(message);
                },
            },
            pool: {
                afterCreate: (conn: Connection, done: (err: MysqlError | null, conn: Connection) => void) => {
                    conn.query('SET session_replication_role = "replica";', (err1) => done(err1, conn));
                },
            },
        }).on('query-error', (err) => {
            this.logger.error(err.code, err.name);
        });

        if (fs.existsSync(this.triggerBackupFile)) {
            this.triggers = fs.readJSONSync(this.triggerBackupFile);
        }
    }

    public async init(): Promise<void> {
        this.logger.warn(`For performance foreign_key_checks, autocommit and unique_checks are disabled during insert.`);
        this.logger.warn(`They are disabled per connections and should not alter your configuration.`);
        this.logger.info(`To improve performances further you can update innodb_autoinc_lock_mode = 0 in your my.ini.`);
    }

    async countLines(table: Table) {
        return (await this.dbConnection(table.name).count())[0]['count(*)'] as number;
    }

    async emptyTable(table: Table) {
        await this.dbConnection.raw(`DELETE FROM \`${table.name}\``);
        await this.dbConnection.raw(`ALTER TABLE \`${table.name}\` AUTO_INCREMENT = 1;`);
    }

    async executeRawQuery(query: string) {
        await this.dbConnection.raw(query);
    }

    async insert(table: string, rows: any[]): Promise<number> {
        if (rows.length === 0) return 0;
        const query = this.dbConnection(table)
            .insert(rows)
            .toQuery()
            .replace('insert into', 'insert ignore into');
        const insertResult = await this.dbConnection.raw(query);
        await this.dbConnection.raw('COMMIT;');
        return insertResult[0].affectedRows;
    }

    async destroy() {
        await this.dbConnection.destroy();
    }

    async getSchema(): Promise<Schema> {
        let tables = await this.getTablesInformation();
        tables = await Promise.all(tables.map(async (table) => {
            await this.extractColumns(table);
            await this.extractForeignKeys(table);
            return table;
        }));
        return Schema.fromJSON({ tables });
    }

    private async extractColumns(table: Table) {
        this.logger.info(table.name);
        const columns: MySQLColumn[] = await this.getColumnsInformation(table);
        columns
            .filter((column: MySQLColumn) => {
                return ['enum', 'set'].includes(column.DATA_TYPE || '');
            }).forEach((column: MySQLColumn) => {
                column.NUMERIC_PRECISION = column.COLUMN_TYPE.match(/(enum|set)\((.*)\)$/)![1].split('\',\'').length;
            });

        table.columns = columns.map((mysqlColumn: MySQLColumn) => {
            const column = new Column();
            column.name = mysqlColumn.COLUMN_NAME;
            if (mysqlColumn.COLUMN_KEY && mysqlColumn.COLUMN_KEY.match(/PRI|UNI/ig)) column.unique = true;
            column.nullable = mysqlColumn.IS_NULLABLE === 'YES' ? 0.1 : 0;
            column.max = mysqlColumn.CHARACTER_MAXIMUM_LENGTH || mysqlColumn.NUMERIC_PRECISION || 255;
            if (mysqlColumn.COLUMN_TYPE && mysqlColumn.COLUMN_TYPE.includes('unsigned')) column.unsigned = true;
            if (mysqlColumn.EXTRA && mysqlColumn.EXTRA.includes('auto_increment')) column.autoIncrement = true;
            switch (mysqlColumn.DATA_TYPE) {
                case 'bool':
                case 'boolean':
                    column.generator = Generators.boolean;
                    break;
                case 'smallint':
                    column.generator = Generators.integer;
                    if (column.unsigned) {
                        column.min = 0;
                        column.max = 65535;
                    } else {
                        column.min = -32768;
                        column.max = 32767;
                    }
                    break;
                case 'mediumint':
                    column.generator = Generators.integer;
                    if (column.unsigned) {
                        column.min = 0;
                        column.max = 16777215;
                    } else {
                        column.min = -8388608;
                        column.max = 8388607;
                    }
                    break;
                case 'tinyint':
                    column.generator = Generators.integer;
                    if (column.unsigned) {
                        column.min = 0;
                        column.max = 255;
                    } else {
                        column.min = -128;
                        column.max = 127;
                    }
                    break;
                case 'int':
                case 'integer':
                case 'bigint':
                    column.generator = Generators.integer;
                    if (column.unsigned) {
                        column.min = 0;
                        column.max = 2147483647;
                    } else {
                        column.min = -2147483648;
                        column.max = 2147483647;
                    }
                    break;
                case 'decimal':
                case 'dec':
                case 'float':
                case 'double':
                    column.generator = Generators.real;
                    if (column.unsigned) {
                        column.min = 0;
                        column.max = 2147483647;
                    } else {
                        column.min = -2147483648;
                        column.max = 2147483647;
                    }
                    break;
                case 'date':
                case 'datetime':
                case 'timestamp':
                    column.generator = Generators.date;
                    column.minDate = '01-01-1970';
                    column.maxDate = undefined;
                    break;
                case 'time':
                    column.generator = Generators.time;
                    break;
                case 'year':
                    column.generator = Generators.integer;
                    column.min = 1901;
                    column.max = 2155;
                    break;
                case 'varchar':
                case 'char':
                case 'binary':
                case 'varbinary':
                case 'tinyblob':
                case 'text':
                case 'tinytext':
                case 'mediumtext':
                case 'longtext':
                case 'blob':
                case 'mediumblob': // 16777215
                case 'longblob': // 4,294,967,295
                    column.generator = Generators.string;
                    break;
                case 'bit':
                case 'set':
                    column.generator = Generators.bit;
                    column.max = mysqlColumn.NUMERIC_PRECISION;
                    break;
                case 'enum':
                    column.generator = Generators.integer;
                    column.max = mysqlColumn.NUMERIC_PRECISION;
                    break;
            }
            return column;
        });
    }

    private extractForeignKeys = async (table: Table) => {
        const foreignKeys = await this.getForeignKeys(table);
        table.referencedTables = [];
        for (const column of table.columns) {
            const match = foreignKeys.find((fk) => fk.column.toLowerCase() === column.name.toLowerCase());
            if (match) {
                column.generator = Generators.foreignKey;
                column.foreignKey = { table: match.foreignTable, column: match.foreignColumn };
                column.unique = column.unique || match.uniqueIndex || false;
                table.referencedTables.push(column.foreignKey.table);
            }
        }
    };

    public async backupTriggers(tables: string[]): Promise<void> {
        const triggers = await this.dbConnection
            .select()
            .from('information_schema.TRIGGERS')
            .where('event_object_schema', this.database)
            .whereIn(`event_object_table`, tables);
        this.triggers = this.triggers.concat(triggers);
        fs.writeJSONSync(this.triggerBackupFile, this.triggers);
    }

    public cleanBackupTriggers(): void {
        fs.unlinkSync(this.triggerBackupFile);
    }

    public async disableTriggers(table: string): Promise<void> {
        const triggers = this.triggers.filter((trigger) => {
            return trigger.EVENT_OBJECT_SCHEMA === this.database && trigger.EVENT_OBJECT_TABLE === table;
        });
        const promises = triggers.map((trigger) => {
            return this.dbConnection.raw(`DROP TRIGGER IF EXISTS ${trigger.TRIGGER_SCHEMA}.${trigger.TRIGGER_NAME};`);
        });
        await Promise.all(promises)
            .catch(err => this.logger.error(err.message));
    }

    public async enableTriggers(table: string): Promise<void> {
        for (let i = 0; i < this.triggers.length; i++) {
            const trigger = this.triggers[i];
            if (trigger.EVENT_OBJECT_SCHEMA !== this.database || trigger.EVENT_OBJECT_TABLE !== table) continue;
            await this.dbConnection.raw(`DROP TRIGGER IF EXISTS ${trigger.TRIGGER_SCHEMA}.${trigger.TRIGGER_NAME};`);
            await this.dbConnection.raw(
                `CREATE DEFINER = ${trigger.DEFINER}
                TRIGGER ${trigger.TRIGGER_SCHEMA}.${trigger.TRIGGER_NAME} ${trigger.ACTION_TIMING} ${trigger.EVENT_MANIPULATION}
                ON ${trigger.EVENT_OBJECT_SCHEMA}.${trigger.EVENT_OBJECT_TABLE}
                FOR EACH ROW
                ${trigger.ACTION_STATEMENT}`,
            );
            this.triggers.splice(i, 1);
        }
    }

    async getTablesInformation(): Promise<Table[]> {
        const tableNames = await this.dbConnection
            .select<{ name: string; }[]>([
                this.dbConnection.raw('t.table_name AS name'),
            ])
            .from('information_schema.tables as t')
            .where('t.table_schema', this.database)
            .andWhere('t.table_type', 'BASE TABLE')
            .groupBy('t.table_schema', 't.table_name');
        return tableNames.map((row) => {
            const table = new Table();
            table.name = row.name;
            return table;
        });
    }

    async getColumnsInformation(table: Table) {
        return this.dbConnection.select()
            .from('information_schema.columns')
            .where({
                'table_schema': this.database,
                'table_name': table.name,
            });
    }

    async getForeignKeys(table: Table) {
        const subQuery = this.dbConnection
            .select([
                'kcu2.table_name',
                this.dbConnection.raw('1 AS unique_index'),
            ])
            .from('information_schema.key_column_usage AS kcu2')
            .innerJoin('information_schema.table_constraints AS tc', function () {
                this.on('tc.constraint_schema', '=', 'kcu2.constraint_schema')
                    .andOn('tc.table_name', '=', 'kcu2.table_name')
                    .andOn('tc.constraint_name', '=', 'kcu2.constraint_name')
                    .andOnIn('tc.constraint_type', ['PRIMARY KEY', 'UNIQUE']);
            })
            .groupBy(['kcu2.table_name', 'kcu2.constraint_name'])
            .having(this.dbConnection.raw('count(kcu2.constraint_name) < 2'))
            .as('indexes');

        return this.dbConnection.select([
            'kcu.column_name AS column',
            'k2.table_name AS foreignTable',
            'k2.column_name AS foreignColumn',
            'unique_index AS uniqueIndex'
        ])
            .from('information_schema.key_column_usage as kcu')
            .leftJoin('information_schema.referential_constraints AS fk', function () {
              this.using(["constraint_schema", "constraint_name"])
            })
          .leftJoin('information_schema.key_column_usage AS k2', function () {
              this.on('k2.constraint_schema', 'fk.unique_constraint_schema')
                .andOn('k2.constraint_name', 'fk.unique_constraint_name')
                .andOn('k2.ordinal_position', 'kcu.position_in_unique_constraint')
          })
            .leftJoin(subQuery, function () {
                this.on('kcu.table_name', 'indexes.table_name')
            })
            .where('kcu.table_name', table.name)
            .whereNotNull('k2.column_name');

        /**
         * destination query
         *
         * SELECT
            k1.table_schema,
            k1.table_name,
            k1.column_name,
            k2.table_schema AS referenced_table_schema,
            k2.table_name AS referenced_table_name,
            k2.constraint_name AS referenced_constraint_name,
            k2.column_name AS referenced_column_name
        FROM information_schema.key_column_usage k1
            JOIN information_schema.referential_constraints fk USING (constraint_schema, constraint_name)
            JOIN information_schema.key_column_usage k2
                ON k2.constraint_schema = fk.unique_constraint_schema
                AND k2.constraint_name = fk.unique_constraint_name
                AND k2.ordinal_position = k1.position_in_unique_constraint
            LEFT JOIN (
             SELECT
                k2.table_name,
                k2.constraint_name
             FROM information_schema.key_column_usage k2
                inner join "information_schema"."table_constraints" as "tc" on "tc"."constraint_schema" = "k2"."constraint_schema"
                    and "tc"."table_name" = "k2"."table_name"
                    and "tc"."constraint_name" = "k2"."constraint_name"
                    and "tc"."constraint_type" in ('PRIMARY KEY', 'UNIQUE')
             GROUP BY k2.table_name, k2.constraint_name
             having count(k2.constraint_name) < 2
         ) as indexes on k1.table_name = indexes.table_name
         WHERE k2.column_name IS NOT NULL AND k1.table_name = 'FsiAgentDelegues';
         */
    }

    async getValuesForForeignKeys(
        table: string,
        column: string,
        foreignTable: string,
        foreignColumn: string,
        limit: number,
        unique: boolean,
        condition: string,
    ) {
        let values = [];
        const query = this.dbConnection(foreignTable)
            .distinct(`${foreignTable}.${foreignColumn}`)
            .limit(limit);
        if (condition) {
            query.andWhere(this.dbConnection.raw(condition));
        }
        if (unique) {
            query.leftJoin(table, function () {
                this.on(`${table}.${column}`, `${foreignTable}.${foreignColumn}`);
            }).whereNull(`${table}.${column}`);
        }
        values = (await query).map(result => result[foreignColumn]);
        return values;
    }
}