import { CustomizedColumn, CustomizedTable } from '../schema/customized-schema.class';
import { BitGenerator } from './generators/bit.generator';
import { BooleanGenerator } from './generators/boolean.generator';
import { DateGenerator } from './generators/date.generator';
import { FakerGenerator } from './generators/faker.generator';
import { ForeignKeyGenerator } from './generators/foreignkey.generator';
import { FunctionGenerator } from './generators/function.generator';
import { Generators } from './generators/generators';
import { IntegerGenerator } from './generators/integer.generator';
import { RealGenerator } from './generators/real.generator';
import { StringGenerator } from './generators/string.generator';
import { TimeGenerator } from './generators/time.generator';
import { ValuesGenerator } from './generators/values.generator';
import { ArrayGenerator } from './generators/postgresql/array.generator';
import { IntervalGenerator } from './generators/postgresql/interval.generator';

export class GeneratorValidation {
    static validate(table: CustomizedTable, column: CustomizedColumn): boolean {
        switch (column.generator) {
            case Generators.bit:
                BitGenerator.validate(table, column);
                break;
            case Generators.boolean:
                BooleanGenerator.validate(table, column);
                break;
            case Generators.integer:
                IntegerGenerator.validate(table, column);
                break;
            case Generators.interval:
                IntervalGenerator.validate(table, column);
                break;
            case Generators.real:
                RealGenerator.validate(table, column);
                break;
            case Generators.date:
                DateGenerator.validate(table, column);
                break;
            case Generators.time:
                TimeGenerator.validate(table, column);
                break;
            case Generators.uuid:
            case Generators.string:
                StringGenerator.validate(table, column);
                break;
            case Generators.values:
                ValuesGenerator.validate(table, column);
                break;
            case Generators.array:
                ArrayGenerator.validate(table, column);
                break;
            case Generators.foreignKey:
                ForeignKeyGenerator.validate(table, column);
                break;
            case Generators.function:
                FunctionGenerator.validate(table, column);
                break;
            case Generators.faker:
                FakerGenerator.validate(table, column);
                break;
            case Generators.none:
            default:
                throw new Error(`No generator defined for column: ${table.name}.${column.name}`);
        }
        return true;
    }
}