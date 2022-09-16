import { Random } from 'random-js';
import { Generators } from '../../../../../src/generation/generators/generators';
import { CustomizedTable, CustomizedColumn } from '../../../../../src/schema/customized-schema.class';
import { Builder } from '../../../../../src/builder';
import { UuidGenerator } from '../../../../../src/generation/generators/postgresql/uuid.generator';


describe('UuidGenerator', () => {
    let random: Random;
    beforeEach(() => {
        random = new Random();
    })
    afterAll(async () => {
        jest.clearAllMocks();
    });
    describe('When generator type is uuid', () => {
        it('should generate uuid value', () => {
            // given
            const expectedValue = '157bb177-de57-4de5-a5b4-725bda45882a';

            const column: CustomizedColumn = new Builder(CustomizedColumn)
              .set('generator', Generators.uuid)
              .build();
            const table: CustomizedTable = new Builder(CustomizedTable)
              .set('columns', [column])
              .build();
            const generator = new UuidGenerator(random, table, column);

            jest.spyOn(random, 'uuid4').mockReturnValue(expectedValue);

            // when
            const result = generator.generate(0, {});

            // then
            expect(result).toBe(expectedValue);
        });
    });
});