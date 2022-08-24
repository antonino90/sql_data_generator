import { AbstractGenerator } from './generators';

export class ArrayGenerator extends AbstractGenerator<string | number | null> {
    validate() {
        if (this.column.max === undefined) throw new Error(`max attribute required for type array: ${this.table.name}.${this.column.name}`);
        if (this.column.arrayElementType === undefined) throw new Error(`arrayElementType attribute required for type array: ${this.table.name}.${this.column.name}`);
        return true;
    }

    generate(rowIndex: number, row: { [key: string]: any; }) {
        const nbValue = this.random.integer(1, this.column.max);
        const templateValues = [...Array(nbValue).keys()]
          .map(() => {
              switch (this.column.arrayElementType) {
                  default:
                  case 'text':
                      return this.random.string(5);
                  case 'int':
                      return this.random.uint32().toFixed(0).slice(0, 5);
              }
          })
          .join(',');

        return `{ ${templateValues} }`;
    }
}