import { AbstractGenerator } from './generators';

export class TimeGenerator extends AbstractGenerator<string> {
    generate(rowIndex: number, row: { [key: string]: any; }) {
        // todo generate cuxtom generator or postgresql (by engine en fait mec)
        // to do like lysql il faut implementer le type de colum 'interval'
        // todo ajouter interval dans le patch sql de postgres

        // version postgresql
        const hours = this.random.integer(-0, +23);
        const minutes = this.random.integer(-0, +59);
        const seconds = this.random.integer(-0, +59);
        return `${hours}:${minutes}:${seconds}`;

        // version mariaaaaa db
        /*const hours = this.random.integer(-838, +838);
        const minutes = this.random.integer(-0, +59);
        const seconds = this.random.integer(-0, +59);
        return `${hours}:${minutes}:${seconds}`;
         */
    }
}