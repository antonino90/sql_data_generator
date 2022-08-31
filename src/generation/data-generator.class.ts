import * as fs from 'fs-extra';
import { getLogger } from 'log4js';
import * as path from 'path';
import * as JSONC from 'jsonc-parser';
import { parse } from 'uri-js';
import { execSync } from 'child_process';
import cliProgress, { SingleBar } from 'cli-progress';
import colors from 'colors';
import { KeyPress, Modifiers } from '@corteks/clify';

import { DatabaseConnector } from '../database/database-connector-builder';
import { CustomSchema } from '../schema/custom-schema.class';
import { Schema } from '../schema/schema.validator';
import { CustomizedSchema } from '../schema/customized-schema.class';
import { Filler, ProgressEvent } from './filler';

const logger = getLogger();
logger.level = 'debug';

export class DataGeneratorClass {
    private readonly schema: string;
    private readonly uri: string | undefined;
    private filler: Filler | undefined;

    constructor(schema: string = 'schema', uri: string | undefined) {
        this.schema = schema;
        this.uri = uri;
    }

    public async generateDataInDB(dbConnector: DatabaseConnector | undefined, shouldResetDataBeforeInsert: boolean = false) {
        if (!dbConnector) throw new Error('DB connection not ready');
        const schema = await this.getSchema();
        const customSchema = this.getCustomSchema();

        try {
            await this.runScripts();
        } catch (ex) {
            logger.error('An error occured while running scripts:', (ex as Error).message);
            return;
        }

        await dbConnector.backupTriggers(customSchema.tables.filter(table => table.maxLines || table.addLines).map(table => table.name));

        this.filler = new Filler(dbConnector, CustomizedSchema.create(schema, customSchema), this.progressEventHandler());
        await this.filler.fillTables(shouldResetDataBeforeInsert);

        dbConnector.cleanBackupTriggers();
    }

    @KeyPress('n', Modifiers.NONE, 'Skip the current table. Only works during data generation phase.')
    skipTable() {
        if (!this.filler) return;
        logger.info('Skipping...');
        this.filler.gotoNextTable();
    }

    private async getSchema(): Promise<Schema> {
        return await Schema.fromJSON(fs.readJSONSync(path.join('settings', `${this.schema}.json`)));
    }

    private getCustomSchema():CustomSchema {
        try {
            return JSONC.parse(fs.readFileSync(path.join('settings', `${this.schema}_custom.jsonc`)).toString());
        } catch (ex) {
            logger.warn(`Unable to read ./settings/${this.schema}_custom.json, this will not take any customization into account.`);
        }
        return new CustomSchema();
    }

    private static getCustomSqlScripts(): string[] {
        const scriptsFolder = path.join('settings', 'scripts');
        if (!fs.existsSync(scriptsFolder)) {
            fs.mkdirSync(scriptsFolder);
            logger.info('No scripts provided.');
            return [];
        }
        return fs.readdirSync(scriptsFolder);
    }

    private async runScripts(scriptsExtension = '.sql') {
        const scripts = DataGeneratorClass.getCustomSqlScripts();
        if (scripts.length === 0) {
            logger.info('No scripts provided.');
            return false;
        }

        const parsedUri = parse(this.uri!);
        for (const script of scripts) {
            if (!script.endsWith(scriptsExtension)) continue;
            logger.info(`Running script: ${script}`);
            execSync(`mysql -h ${parsedUri.host!} --port=${parsedUri.port!.toString()} --protocol=tcp --default-character-set=utf8 -c -u ${parsedUri.userinfo!.split(':')[0]} -p"${parsedUri.userinfo!.split(':')[1]}" ${parsedUri.path?.replace('/', '')} < "${script}"`, {
                cwd: path.join('settings', 'scripts'),
                stdio: 'pipe',
            });
        }
    }

    private progressEventHandler() {
        let previousEvent: ProgressEvent = { currentTable: '', currentValue: 0, max: 0, state: 'DONE', step: '' };
        let currentProgress: SingleBar;
        return (event: ProgressEvent) => {
            if (DataGeneratorClass.hasChangesDetected(previousEvent, event)) {
                if (currentProgress) currentProgress.stop();
                currentProgress = new cliProgress.SingleBar({
                    format: `${event.step + new Array(16 - event.step.length).join(' ')} | ${colors.cyan('{bar}')} | {percentage}% | {value}/{total} | {comment}`,
                    stopOnComplete: true,
                });
                currentProgress.start(event.max, event.currentValue, { comment: event.comment || '' });
            } else {
                event.comment = [previousEvent.comment, event.comment].join('');
                if (currentProgress) currentProgress.update(event.currentValue, { comment: event.comment });
                if (event.state === 'DONE') currentProgress.stop();
            }
            previousEvent = event;
        };
    }

    private static hasChangesDetected(previousEvent: ProgressEvent, event: ProgressEvent) {
        if (previousEvent.currentTable !== event.currentTable) {
            logger.info(colors.green(event.currentTable));
            return true;
        }

        return previousEvent.step !== event.step;
    }
}