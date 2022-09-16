import { AbstractGenerator } from '../generators';

export class TimeGenerator extends AbstractGenerator<string> {
    generate(rowIndex: number, row: { [key: string]: any; }) {
        // to do like lysql il faut implementer le type de colum 'interval'
        // todo ajouter interval dans le patch sql de postgres

        // version postgresql
        const hours = this.random.integer(-0, +23);
        const minutes = this.random.integer(-0, +59);
        const seconds = this.random.integer(-0, +59);
        return `${hours}:${minutes}:${seconds}`;
    }
}