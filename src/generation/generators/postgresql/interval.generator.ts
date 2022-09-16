import { AbstractGenerator } from '../generators';

export class IntervalGenerator extends AbstractGenerator<string> {
    generate(rowIndex: number, row: { [key: string]: any; }) {
        const years = this.random.integer(0, 99);
        const months = this.random.integer(0, 12);
        const days = this.random.integer(0, 365);
        const hours = this.random.integer(-0, +23);
        const minutes = this.random.integer(-0, +59);
        const seconds = this.random.integer(-0, +59);

        return `${years} year ${months} month ${days} day ${hours}:${minutes}:${seconds}`;
    }
}