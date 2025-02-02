import { ForeignKey, Schema, Table } from '../schema/schema.validator';
import { DatabaseEngines } from './database-engines';
import { MariaDBConnector } from './mariadb-connector';
import { PostgresConnector } from './postgres-connector';

export interface DatabaseConnector {
    init(): Promise<void>;
    destroy(): Promise<void>;
    countLines(table: Table): Promise<number>;
    emptyTable(table: Table): Promise<void>;
    executeRawQuery(query: string): Promise<void>;
    insert(table: string, lines: any[]): Promise<number>;
    getSchema(): Promise<Schema>;

    getTablesInformation(): Promise<Table[]>;
    getColumnsInformation(table: Table): Promise<MySQLColumn[] | PostgreSQLColumn[]>;
    getForeignKeys(table: Table): Promise<ForeignKey[]>;
    getValuesForForeignKeys(
        table: string,
        column: string,
        foreignTable: string,
        foreignColumn: string,
        limit: number,
        unique: boolean,
        condition: string | undefined,
    ): Promise<any[]>;
    backupTriggers(tables: string[]): Promise<void>;
    cleanBackupTriggers(): void;
    disableTriggers(table: string): Promise<void>;
    enableTriggers(table: string): Promise<void>;
}

export class DatabaseConnectorBuilder {
    constructor(
      private uri: string,
      private databaseSchema: string,
    ) {}

    public async build(engine: string): Promise<DatabaseConnector> {
        let connector: DatabaseConnector;
        switch (engine) {
            case DatabaseEngines.MYSQL:
            case DatabaseEngines.MARIADB:
                connector = new MariaDBConnector(this.uri, this.databaseSchema);
                await connector.init();
                break;
            case DatabaseEngines.POSTGRES:
                connector = new PostgresConnector(this.uri, this.databaseSchema);
                await connector.init();
                break;
            default:
                throw new Error(`Unsupported engine ${engine}.`);
        }
        return connector;
    }
}