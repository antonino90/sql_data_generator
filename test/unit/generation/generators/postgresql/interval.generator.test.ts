import { Random } from 'random-js';
import { Generators } from '../../../../../src/generation/generators/generators';
import { CustomizedTable, CustomizedColumn } from '../../../../../src/schema/customized-schema.class';
import { Builder } from '../../../../../src/builder';
import { IntervalGenerator } from '../../../../../src/generation/generators/postgresql/interval.generator';

describe('IntervalGenerator', () => {
    let random: Random;
    beforeEach(() => {
        random = new Random();
    })
    afterAll(async () => {
        jest.clearAllMocks();
    });
    describe('When generator type is interval', () => {
        it('should generate interval value', () => {
            // given
            const years = 2;
            const months = 11;
            const days = 3;
            const hours = 12;
            const minutes = 0;
            const seconds = 12;

            const expectedValues = [years, months, days, hours, minutes, seconds];
            const expectedValueFinal = '2 year 11 month 3 day 12:0:12';

            const column: CustomizedColumn = new Builder(CustomizedColumn)
              .set('generator', Generators.uuid)
              .build();
            const table: CustomizedTable = new Builder(CustomizedTable)
              .set('columns', [column])
              .build();
            const generator = new IntervalGenerator(random, table, column);

            expectedValues.forEach((v) => jest.spyOn(random, 'integer').mockReturnValueOnce(v))

            // when
            const result = generator.generate(0, {});

            // then
            expect(result).toBe(expectedValueFinal);
        });
    });
});