import { Random } from 'random-js';

import { DatabaseConnector } from '../database/database-connector-builder';
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
import { ValuesGenerator } from './generators/values.generator';
import { UuidGenerator } from './generators/postgresql/uuid.generator';
import { ArrayGenerator } from './generators/postgresql/array.generator';
import { PostgresConnector } from '../database/postgres-connector';
import { TimeGenerator } from './generators/time.generator';
import { IntervalGenerator } from './generators/postgresql/interval.generator';
import { TimeGenerator as psql_TimeGenerator } from './generators/postgresql/time.generator';

export class GeneratorBuilder {
    constructor(
        private random: Random,
        private dbConnector: DatabaseConnector,
        private table: CustomizedTable,
    ) {

    }

    build(
        column: CustomizedColumn,
    ) {
        switch (column.generator) {
            case Generators.bit:
                return new BitGenerator(this.random, this.table, column);
            case Generators.boolean:
                return new BooleanGenerator(this.random, this.table, column);
            case Generators.integer:
                return new IntegerGenerator(this.random, this.table, column);
            case Generators.real:
                return new RealGenerator(this.random, this.table, column);
            case Generators.date:
                return new DateGenerator(this.random, this.table, column);
            case Generators.time:
                if (this.dbConnector instanceof PostgresConnector) {
                    return new psql_TimeGenerator(this.random, this.table, column);
                }
                return new TimeGenerator(this.random, this.table, column);
            case Generators.string:
                return new StringGenerator(this.random, this.table, column);
            case Generators.interval:
                if (this.dbConnector instanceof PostgresConnector) {
                    return new IntervalGenerator(this.random, this.table, column);
                }
                break;
            case Generators.uuid:
                if (this.dbConnector instanceof PostgresConnector) {
                    return new UuidGenerator(this.random, this.table, column);
                }
                break;
            case Generators.values:
                return new ValuesGenerator(this.random, this.table, column);
            case Generators.foreignKey:
                const generator =  new ForeignKeyGenerator(this.random, this.table, column);
                (generator as ForeignKeyGenerator).setDbConnector(this.dbConnector);
                return generator;
            case Generators.function:
                return new FunctionGenerator(this.random, this.table, column);
            case Generators.faker:
                return new FakerGenerator(this.random, this.table, column);
            case Generators.array:
                if (this.dbConnector instanceof PostgresConnector) {
                    return new ArrayGenerator(this.random, this.table, column);
                }
                break;
            case Generators.none:
            default:
        }

        throw new Error(`No generator defined for column: ${this.table.name}.${column.name}`);
    }
}