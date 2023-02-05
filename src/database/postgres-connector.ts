import * as fs from 'fs-extra';
import Knex, { Knex as KnexInterface } from 'knex';
import { getLogger } from 'log4js';
import { PoolClient } from 'pg';
import * as path from 'path';

import { Generators } from '../generation/generators/generators';
import { PostgresColumn, Schema, Table } from '../schema/schema.validator';
import { DatabaseConnector } from './database-connector-builder';

export class PostgresConnector implements DatabaseConnector {
    private dbConnection: KnexInterface;
    private triggers: PostgreSqlTrigger[] = [];
    private logger = getLogger();
    private triggerBackupFile: string = path.join('settings', 'triggers.json');

    constructor(
        private uri: string,
        private database: string,
    ) {
        this.dbConnection = Knex({
            client: 'postgres',
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
                afterCreate: (client: PoolClient, done: (err: Error | null, client: PoolClient) => void) =>
                  client.query('SET session_replication_role = "replica";', (err1) => {
                      if (err1) done(err1, client)
                      else
                          client.query('SET enable_seqscan TO off', (err2) => done(err2, client))
                  })
                ,
            },
        }).on('query-error', (err) => {
            this.logger.error(err.code, err.name);
        });

        if (fs.existsSync(this.triggerBackupFile)) {
            this.triggers = fs.readJSONSync(this.triggerBackupFile);
        }
    }

    public async init(): Promise<void> {
        this.logger.warn(`For performance session_replication_role, enable_seqscan are disabled during insert.`);
        this.logger.warn(`They are disabled per connections and should not alter your configuration.`);
    }

    async countLines(table: Table) {
        const getTotalCount = await this.dbConnection(table.name).count().first();
        return (getTotalCount?.count) ? Number(getTotalCount.count) : 0;
    }

    async emptyTable(table: Table) {
        await this.dbConnection.raw(`TRUNCATE TABLE "${table.name}" RESTART IDENTITY CASCADE`);
    }

    async executeRawQuery(query: string) {
        await this.dbConnection.raw(query);
    }

    async insert(table: string, rows: any[]): Promise<number> {
        if (rows.length === 0) return 0;
        const insertResult = await this.dbConnection.raw(
          `? ON CONFLICT DO NOTHING;`,
          [this.dbConnection(table).insert(rows)],
        );
        await this.dbConnection.raw('COMMIT;');
        return Number(insertResult.rowCount);
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
        const columns: PostgreSQLColumn[] = await this.getColumnsInformation(table);
        columns
            .filter((column: PostgreSQLColumn) => [
              'enum',
              // 'set' // @todo manage equivalent of SET type of sql
            ].includes(column.data_type || ''))
            .forEach((column: PostgreSQLColumn) => {
                if (column.enum_values) {
                    return column.numeric_precision = (column.enum_values).split(',').length;
                }
            });

        table.columns = columns.map((postgresqlColumn: PostgreSQLColumn) => {
            const column = new PostgresColumn();
            column.name = postgresqlColumn.column_name;
            if (postgresqlColumn.column_key && postgresqlColumn.column_key.match(/PRI|UNI/ig)) column.unique = true;
            column.nullable = postgresqlColumn.is_nullable === 'YES' ? 0.1 : 0;
            column.max = postgresqlColumn.character_maximum_length || postgresqlColumn.numeric_precision || 255;
            if (postgresqlColumn.extra && postgresqlColumn.extra.includes('auto_increment')) column.autoIncrement = true;

            switch (postgresqlColumn.data_type) {
                case 'bool':
                case 'boolean':
                    column.generator = Generators.boolean;
                    break;
                case 'smallint':
                    column.generator = Generators.integer;
                    column.min = -32768;
                    column.max = 32767;
                    break;
                case 'mediumint':
                    column.generator = Generators.integer;
                    column.min = -8388608;
                    column.max = 8388607;
                    break;
                case 'tinyint':
                    column.generator = Generators.integer;
                    column.min = -128;
                    column.max = 127;
                    break;
                case 'int':
                case 'integer':
                case 'bigint':
                case 'bigserial':
                    column.generator = Generators.integer;
                    column.min = -2147483648;
                    column.max = 2147483647;
                    break;
                case 'numeric':
                case 'decimal':
                case 'float8':
                case 'double precision':
                case 'double':
                    column.generator = Generators.real;
                    column.min = -2147483648;
                    column.max = 2147483647;
                    break;
                case 'date':
                case 'datetime':
                case 'timestamp with time zone':
                case 'timestamp without time zone':
                    column.generator = Generators.date;
                    column.minDate = '01-01-1970';
                    column.maxDate = undefined;
                    break;
                case 'time with time zone':
                case 'time without time zone':
                case 'interval':
                    column.generator = Generators.time;
                    break;
                case 'uuid':
                    column.generator = Generators.uuid;
                    break;
                case 'character':
                case 'character varying':
                case 'text':
                    column.generator = Generators.string;
                    break;
                case 'bit':
                case 'bit varying':
                    column.generator = Generators.bit;
                    column.max = postgresqlColumn.numeric_precision;
                    break;
                case 'array':
                    column.generator = Generators.array;
                    column.arrayElementType = postgresqlColumn.element_array_data_type;
                    column.max = 5;
                    break;
                case 'enum':
                    column.generator = Generators.values;
                    column.values = postgresqlColumn.enum_values?.split(',').map((v) => v.trim());
                    column.max = postgresqlColumn.numeric_precision;
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

    private extractTriggers = async (tables: string[]) => {
        return this.dbConnection
          .select()
          .from('information_schema.triggers')
          .where('event_object_schema', this.database)
          .whereIn(`event_object_table`, tables);
    }

    public async backupTriggers(tables: string[]): Promise<void> {
        const triggers = await this.extractTriggers(tables);
        this.triggers = this.triggers.concat(triggers);
        fs.writeJSONSync(this.triggerBackupFile, this.triggers);
    }

    public cleanBackupTriggers(): void {
        fs.unlinkSync(this.triggerBackupFile);
    }

    public async disableTriggers(table: string): Promise<void> {
        const triggers = this.triggers.filter((trigger) => {
            return trigger.event_object_schema === this.database && trigger.event_object_table === table;
        });
        const promises = triggers.map((trigger) => {
            return this.dbConnection.raw(`DROP TRIGGER IF EXISTS ${trigger.trigger_schema}.${trigger.trigger_name};`);
        });
        await Promise.all(promises)
            .catch(err => this.logger.error(err.message));
    }

    public async enableTriggers(table: string): Promise<void> {
        for (let i = 0; i < this.triggers.length; i++) {
            const trigger = this.triggers[i];
            if (trigger.event_object_schema !== this.database || trigger.event_object_table !== table) continue;
            await this.dbConnection.raw(`DROP TRIGGER IF EXISTS ${trigger.trigger_schema}.${trigger.trigger_name};`);
            await this.dbConnection.raw(
                `CREATE DEFINER = ${trigger.definer}
                TRIGGER ${trigger.trigger_schema}.${trigger.trigger_name} ${trigger.action_timing} ${trigger.event_manipulation}
                ON ${trigger.event_object_schema}.${trigger.event_object_table}
                FOR EACH ROW
                ${trigger.action_statement}`,
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

    async getColumnsInformation(table: Table): Promise<PostgreSQLColumn[]> {
        let columnsData = await this.dbConnection.select<PostgreSQLColumn[]>()
            .from('information_schema.columns')
            .where({
                'table_schema': this.database,
                'table_name': table.name,
            });

        const columnsConstraint = await this.getColumnsConstraints(table);
        if (columnsConstraint.length) {
            columnsData = columnsData.map((column) => {
                const match = columnsConstraint.find((col) => column.column_name.toLowerCase() === col.column_name.toLowerCase().replace(/"/gi, ''));
                if (match) {
                    column.column_key = (match.indisprimary ? 'PRIMARY KEY' : 'UNIQUE');
                }
                return column;
            })
        }

        const columnsWithEnumType = await this.getColumnsWithEnumType(table);
        if (columnsWithEnumType.length) {
            columnsData = columnsData.map((column) => {
                const match = columnsWithEnumType.find((col) => {
                    return (
                      column.table_name.toLowerCase() === col.table_name.toLowerCase() &&
                      column.column_name.toLowerCase() === col.column_name.toLowerCase()
                    );
                });

                if (match) {
                    column.data_type = 'enum';
                    column.enum_values = match.enum_values;
                }
                return column;
            })
        }

        const columnsWithAutoIncrement = await this.getColumnsWithAutoIncrement(table);
        if (columnsWithAutoIncrement.length) {
            columnsData = columnsData.map((column) => {
                const match = columnsWithAutoIncrement.find((col) => {
                    return (
                      column.table_name.toLowerCase() === col.table_name.toLowerCase() &&
                      column.column_name.toLowerCase() === col.column_name.toLowerCase()
                    );
                });

                if (match) {
                    column.extra = 'auto_increment';
                }
                return column;
            });
        }

        const hasColumnWithTypeARRAY = columnsData.some((c) => c.data_type === 'ARRAY');
        if (hasColumnWithTypeARRAY) {
            const detailsForColumnTypeARRAY = await this.getDetailsForColumnTypeARRAY(table);
            if (detailsForColumnTypeARRAY.length) {
                columnsData = columnsData.map((column) => {
                    const match = detailsForColumnTypeARRAY.find((col) => {
                        return (
                          column.table_name.toLowerCase() === col.table_name.toLowerCase() &&
                          column.column_name.toLowerCase() === col.column_name.toLowerCase()
                        );
                    });

                    if (match) {
                        column.data_type = 'array';
                        column.element_array_data_type = match.element_array_data_type.match(
                          /int|numeric|decimal|double/ig,
                        ) ? 'int' : 'text';
                    }

                    return column;
                })
            }
        }

        return columnsData;
    }

    private async getColumnsConstraints(table: Table): Promise<ColumnConstraintQueryType[]> {
        return this.dbConnection
          .select<ColumnConstraintQueryType[]>([
              't.relname as table_name',
              'ix.relname as index_name',
              this.dbConnection.raw('regexp_replace(pg_get_indexdef(indexrelid), \'.*\\((.*)\\)\', \'\\1\') as column_name'),
              this.dbConnection.raw('indisunique'),
              this.dbConnection.raw('indisprimary'),
          ])
          .from('pg_index AS i')
          .join('pg_class AS t', function () {
              this.on('t.oid', '=', 'i.indrelid')
          })
          .join('pg_class AS ix', function () {
              this.on('ix.oid', '=', 'i.indexrelid')
          })
          .where('t.relname', table.name)
          .andWhereRaw('(indisunique OR indisprimary)');
    }

    private async getColumnsWithEnumType(table: Table): Promise<ColumnEnumQueryType[]> {
        return this.dbConnection
          .select<ColumnEnumQueryType[]>([
              'col.table_name',
              'col.column_name',
              this.dbConnection.raw('string_agg(enu.enumlabel, \', \' order by enu.enumsortorder) as enum_values'),
          ])
          .from('information_schema.columns AS col')
          .join('information_schema.tables AS tab', function () {
              this.on('tab.table_schema', '=', 'col.table_schema')
              this.andOn('tab.table_name', '=', 'col.table_name')
          })
          .join('pg_type AS typ', function () {
              this.on('col.udt_name', '=', 'typ.typname')
          })
          .join('pg_enum AS enu', function () {
              this.on('typ.oid', '=', 'enu.enumtypid')
          })
          .whereNotIn('col.table_schema', ['information_schema', 'pg_catalog'])
          .andWhere('col.table_name', table.name)
          .andWhere('typ.typtype', 'e')
          .andWhere('tab.table_type', 'BASE TABLE')
          .groupBy(['col.table_name', 'col.column_name']);
    }

    private async getColumnsWithAutoIncrement(table: Table): Promise<ColumnAutoIncrementQueryType[]> {
        return this.dbConnection
          .select<ColumnAutoIncrementQueryType[]>([
              'col.table_name',
              'col.column_name',
          ])
          .from('information_schema.columns AS col')
          .joinRaw('INNER JOIN information_schema.sequences AS seq ON seq.sequence_name = REGEXP_REPLACE(substring(pg_get_serial_sequence(concat(\'"\', table_name, \'"\'), column_name), 8), \'"\', \'\', \'g\')')
          .where('col.table_schema', this.database)
          .andWhere('col.table_name', table.name)
          .andWhere('seq.increment', '1');
    }

    private async getDetailsForColumnTypeARRAY(table: Table): Promise<DetailsForColumnTypeARRAYQueryType[]> {
        const queryParamOnInnerJoin = this.dbConnection.raw('?', ['TABLE']);
        return this.dbConnection
          .select<DetailsForColumnTypeARRAYQueryType[]>([
              'col.table_name',
              'col.column_name',
              'col.data_type',
              'ele.data_type AS element_array_data_type',
          ])
          .from('information_schema.columns AS col')
          .innerJoin('information_schema.element_types AS ele', function() {
              this.on('col.table_catalog', 'ele.object_catalog')
                .andOn('col.table_schema', 'ele.object_schema')
                .andOn('ele.object_type', queryParamOnInnerJoin)
                .andOn('col.dtd_identifier', 'ele.collection_type_identifier')
          })
          .where('col.table_schema', this.database)
          .andWhere('col.table_name', table.name);
    }

    async getForeignKeys(table: Table) {
/***
 * SELECT k1.table_schema,
       k1.table_name,
       k1.column_name,
       k2.table_schema AS referenced_table_schema,
       k2.table_name AS referenced_table_name,
       k2.column_name AS referenced_column_name
FROM information_schema.key_column_usage k1
JOIN information_schema.referential_constraints fk USING (constraint_schema, constraint_name)
JOIN information_schema.key_column_usage k2
  ON k2.constraint_schema = fk.unique_constraint_schema
 AND k2.constraint_name = fk.unique_constraint_name
 AND k2.ordinal_position = k1.position_in_unique_constraint;
 */

        const subQuery = this.dbConnection
            .select([
                'kcu2.table_name',
                this.dbConnection.raw('MAX(kcu2.column_name) as column_name'),
                this.dbConnection.raw('MAX(kcu2.constraint_schema) as constraint_schema'),
                //'kcu2.column_name',
                //'kcu2.constraint_name',
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
            .whereNot('kcu2.table_name', 'LIKE', 'pg_%') // todo dynamic
            .as('indexes')

        return this.dbConnection.select([
            'kcu.column_name AS column',
            'k2.table_name AS foreignTable',
            'k2.column_name AS foreignColumn',
            'unique_index AS uniqueIndex',
        ])
            .from('information_schema.key_column_usage as kcu')
            .leftJoin('information_schema.referential_constraints AS fk', function () {
              this.using(['constraint_schema', 'constraint_name'])
            })
          .leftJoin('information_schema.key_column_usage AS k2', function () {
              this.on('k2.constraint_schema', 'fk.unique_constraint_schema')
                .andOn('k2.constraint_name', 'fk.unique_constraint_name')
                .andOn('k2.ordinal_position', 'kcu.position_in_unique_constraint')
          })
            .leftJoin(subQuery, function () {
                this.on('kcu.table_name', 'indexes.table_name')
                  .andOn('kcu.column_name', 'indexes.column_name')
                  .andOn('kcu.constraint_schema', 'indexes.constraint_schema');
                  //.andOn('kcu.column_name', 'indexes.column_name')
                  //.andOn('kcu.constraint_name', 'indexes.constraint_name');
            })
            .where('kcu.table_name', table.name)
            .whereNotNull('k2.column_name');
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