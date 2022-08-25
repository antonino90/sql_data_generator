import { MersenneTwister19937, Random } from 'random-js';
import { Generators } from '../../../src/generation/generators/generators';
import { CustomizedTable, CustomizedColumn } from '../../../src/schema/customized-schema.class';
import { Builder } from '../../../src/builder';
import { ArrayGenerator } from '../../../src/generation/generators/array.generator';

describe('ArrayGenerator', () => {
    let random: Random;
    beforeEach(() => {
        random = new Random();
    })
    afterAll(async () => {
        jest.clearAllMocks();
    });
    describe('When sub element of array is a type String', () => {
        it('should generate array of string', () => {
            // given
            const expectedValues = ['psI_Y', 'eWbxg'];

            const column: CustomizedColumn = new Builder(CustomizedColumn)
              .set('generator', Generators.array)
              .set('max', expectedValues.length)
              .build();
            const table: CustomizedTable = new Builder(CustomizedTable)
              .set('columns', [column])
              .build();
            const generator = new ArrayGenerator(random, table, column);

            jest.spyOn(random, 'integer').mockReturnValue(expectedValues.length);
            jest.spyOn(random, 'string')
              .mockReturnValueOnce(expectedValues[0])
              .mockReturnValueOnce(expectedValues[1])
            ;

            // when
            const result = generator.generate(0, {});

            // then
            expect(result).toBe('{ ' + expectedValues.join(',') + ' }');
        });
    });

    describe('When sub element of array is a type Int', () => {
        it('should generate array of int', () => {
            // given
            const expectedValues = [60525,23097,13335,33244];
            const column: CustomizedColumn = new Builder(CustomizedColumn)
              .set('generator', Generators.array)
              .set('max', expectedValues.length)
              .set('arrayElementType', 'int')
              .build();
            const table: CustomizedTable = new Builder(CustomizedTable)
              .set('columns', [column])
              .build();
            const generator = new ArrayGenerator(random, table, column);

            jest.spyOn(random, 'integer').mockReturnValue(expectedValues.length);
            jest.spyOn(random, 'uint32')
              .mockReturnValueOnce(expectedValues[0])
              .mockReturnValueOnce(expectedValues[1])
              .mockReturnValueOnce(expectedValues[2])
              .mockReturnValueOnce(expectedValues[3])
            ;

            // when
            const result = generator.generate(0, {});

            // then
            expect(result).toBe('{ ' + expectedValues.join(',') + ' }');
        });
    });
});