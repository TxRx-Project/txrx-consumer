import { IWorker } from '../../src/interfaces/IWorker';
import Worker from '../../src/worker';
import { Consumable, ConsumeItem, ConsumingMode } from '../../types/consumer.types';

export default class DummyWorker extends Worker implements IWorker {
    public group = true;
    public consumingOverride: Consumable;
    public stopsAt = 1;
    public consumptions = 0;

    public consumable(): Consumable {
        if (this.consumingOverride) {
            return this.consumingOverride;
        }

        if (this.group) {
            return {
                count: 100,
                block: 2000,
                stream: 'TEST:STREAM',
                group: 'TEST:GROUP',
                consumer: 'TEST:CONSUMER:0',
                id: '0',
                mode: ConsumingMode.PEL,
            };
        }
        
        return {
            stream: 'TEST:STREAM',
            count: 1,
            block: 2000,
            id: '0',
            mode: ConsumingMode.PEL,
        };
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

        return Object.keys(item.payload).length > 0;
    }
}
