import { MersenneTwister19937, Random } from "random-js";
import { BitGenerator, IntegerGenerator } from "../../../src/generation/generators";
import { Generators } from "../../../src/generation/generators/generators";
import { Column } from "../../../src/schema/schema.class";
import { CustomizedTable } from '../../../src/schema/customized-schema.class';
import { Builder } from '../../../src/builder';

let random = new Random(MersenneTwister19937.seed(42));
describe('IntegerGenerator', () => {
    it('should generate bits', () => {
        const column: Column = new Builder(Column)
            .set('generator', Generators.integer)
            .set('max', 10)
            .build();

        const table: CustomizedTable = new Builder(CustomizedTable)
            .set('columns', [column])
            .build();

        const row = {};

        const generator = new IntegerGenerator(random, table, column);
        expect(generator.generate(0, row)).toBe(6);
    });
});