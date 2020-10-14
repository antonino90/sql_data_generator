import { AbstractGenerator } from "./generators";
import { DatabaseConnector } from '../../database/database-connector-builder';

export class ForeignKeyGenerator extends AbstractGenerator<string | number | undefined> {
    private dbConnector: DatabaseConnector | undefined;
    private values: Array<string | number> = [];

    setDbConnector(dbConnector: DatabaseConnector) {
        this.dbConnector = dbConnector;
    }

    async init() {
        if (!this.dbConnector) throw new Error('Could not connect to database');
        await this.dbConnector.getValuesForForeignKeys(
            this.table.name,
            this.column.name,
            this.column.foreignKey!.table,
            this.column.foreignKey!.column,
            this.table.deltaRows,
            this.column.unique,
            this.column.foreignKey!.where,
        ).then((values) => {
            this.values = this.random.shuffle(values);
        });
    }

    generate(rowIndex: number, row: { [key: string]: any; }) {
        const foreignKeys = this.values;
        if (rowIndex < foreignKeys.length) return foreignKeys[rowIndex];
        else if (!this.column.unique) {
            return foreignKeys[rowIndex % foreignKeys.length];
        } else {
            if (this.column.nullable === false) {
                throw new Error(`Not enough FK for column: ${this.table.name}.${this.column.name}`);
            }
        }
    }
}