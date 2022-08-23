import { AbstractGenerator } from './generators';

export class UuidGenerator extends AbstractGenerator<string> {
    validate() {
        if (this.column.min === undefined) throw new Error(`min value required for type uuid: ${this.table.name}.${this.column.name}`);
        if (this.column.max === undefined) throw new Error(`max value required for type uuid: ${this.table.name}.${this.column.name}`);
        return true;
    }

    generate(rowIndex: number, row: { [key: string]: any; }) {
        return this.random.uuid4();
    }
}