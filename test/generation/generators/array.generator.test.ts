import { MersenneTwister19937, Random } from 'random-js';
import { Generators } from '../../../src/generation/generators/generators';
import { CustomizedTable, CustomizedColumn } from '../../../src/schema/customized-schema.class';
import { Builder } from '../../../src/builder';
import { ArrayGenerator } from "../../../src/generation/generators/array.generator";

const random = new Random(MersenneTwister19937.seed(42));

describe('ArrayGenerator', () => {
    it('should generate array of string', () => {
        const column: CustomizedColumn = new Builder(CustomizedColumn)
            .set('generator', Generators.array)
            .set('max', 1)
            .build();

        const table: CustomizedTable = new Builder(CustomizedTable)
            .set('columns', [column])
            .build();

        const row = {};

        const generator = new ArrayGenerator(random, table, column);
        expect(generator.generate(0, row)).toBe('{ 8uM5s }');
    });
});