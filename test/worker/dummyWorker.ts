import { IWorker } from '../../src/interfaces/IWorker';
import Worker from '../../src/worker';
import { Consumable, ConsumeItem } from '../../types/consumer.types';

export default class DummyWorker extends Worker implements IWorker {
    public group = true;
    public consumingOverride: Consumable;
    public stopsAt = 1;
    public consumptions = 0;
    public toConsume: Consumable;

    public consumable(): Consumable {
        if (this.consumingOverride) {
            return this.consumingOverride;
        }

        if (this.group) {
            this.toConsume = {
                count: 100,
                block: 2000,
                stream: 'TEST:STREAM',
                group: 'TEST:GROUP',
                consumer: 'TEST:CONSUMER:0',
                id: '0',
            };
        } else {
            this.toConsume = {
                stream: 'TEST:STREAM',
                count: 1,
                block: 2000,
                id: '0',
            };
        }

        return this.toConsume;
    }

    protected async consumption(items: ConsumeItem[]) {
        if (this.stopsAt) {
            this.consumptions++;

            if (this.consumptions >= this.stopsAt) {
                this.setRunning(false);
            }
        }

        await super.consumption(items);
    }

    public async consume(item: ConsumeItem): Promise<boolean> {
        if (typeof item.payload.throw !== 'undefined') {
            throw item.payload.throw;
        }

        if (typeof item.payload.set !== 'undefined') {  
            if (this.consumingOverride) {
                this.consumingOverride.id = item.payload.set;
            } else {
                this.toConsume.id = item.payload.set;
            }
        }

        return Object.keys(item.payload).length > 0;
    }
}
