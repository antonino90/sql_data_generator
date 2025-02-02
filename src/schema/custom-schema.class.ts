import { classToPlain, plainToClass, Type } from 'class-transformer';
import { IsArray, IsBoolean, IsEnum, IsNumber, IsOptional, IsString, ValidateNested, validateOrReject } from 'class-validator';
import { DatabaseEngines } from '../database/database-engines';
import { Generators } from '../generation/generators/generators';
import { MariaDbColumn } from './schema.validator';

export class CustomSettings {
    @IsArray()
    @ValidateNested({ each: true })
    beforeAll: string[] = [];
    @IsArray()
    @ValidateNested({ each: true })
    afterAll: string[] = [];
    @IsEnum(DatabaseEngines)
    engine: DatabaseEngines = DatabaseEngines.MARIADB; // @todo should be dynamic value depending on current engine
    @IsBoolean()
    disableTriggers: boolean = false;
    @IsArray()
    @ValidateNested({ each: true })
    ignoredTables: string[] = [];
    @IsArray()
    @ValidateNested({ each: true })
    tablesToFill: string[] = [];
    /**
     * A default value has been provided to maxLengthValue as this can drastically improve performances. This is a parameter which used to be
     * defined a lot by user anyway. The value of 36 has been chosen to allow usage of UID in string generator.
     */
    @IsNumber()
    @IsOptional()
    maxLengthValue?: number = 36;
    values: { [key: string]: any[]; } = {};
    options: {
        generators: Generators[],
        options: Partial<MariaDbColumn>;
    }[] = [];
    @IsNumber()
    @IsOptional()
    seed?: number;
    @IsNumber()
    maxRowsPerBatch: number = 1000;
    @IsNumber()
    minRowsPerTable: number = 1000;
}

export class CustomSchema {
    @ValidateNested()
    settings: CustomSettings = new CustomSettings();
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => CustomTable)
    tables: CustomTable[] = [];

    static async fromJSON(json: any): Promise<CustomSchema> {
        const customSchema = plainToClass(CustomSchema, json);
        try {
            await validateOrReject(customSchema);
        } catch (errors) {
            throw new Error(errors + 'You should regenerate your schema.');
        }
        return customSchema;
    }

    toJSON() {
        return classToPlain(this);
    }
}

export class CustomTable {
    @IsString()
    name: string = '';
    @IsArray()
    @ValidateNested({ each: true })
    columns?: CustomColumn[] = [];
    @IsNumber()
    lines?: number;
    @IsArray()
    @ValidateNested({ each: true })
    before?: string[];
    @IsArray()
    @ValidateNested({ each: true })
    after?: string[];
    @IsNumber()
    maxLines?: number;
    @IsNumber()
    addLines?: number;
    @IsBoolean()
    disableTriggers?: boolean;
    @IsString()
    template?: any;
}

type CustomColumn = { name: string; } & Partial<MariaDbColumn>;
