import { Consumable, ConsumeItem, Consumption } from './../types/consumer.types';
import { RedisConnector } from "@txrx/redis-pool";
import Redis from 'ioredis';

export default class Consumer {
    private redis: Redis;

    constructor(private url: string) {
        this.redis = RedisConnector.get('consumer').get(url);
    }

    public async createGroup(what: Consumable) {
        return this.redis.xgroup('CREATE', what.stream, what.group, '$', 'MKSTREAM');
    }

    public async ack(what: Consumable, which: ConsumeItem | string[]) {
        if (Object.hasOwn(which,'id')) {
            const w = which as ConsumeItem;
            return this.redis.xack(w.stream, what.group, w.id);
        }

        return this.redis.xack(what.stream, what.group, ...(which as string[]));
    }

    public async consume(what: Consumable): Promise<ConsumeItem[]> {
        let consumption: Consumption[] | null;

        if (what.consumer && what.group) {
            consumption = await this.redis.xreadgroup(
                'GROUP', 
                what.group, 
                what.consumer, 
                'COUNT', 
                what.count, 
                'BLOCK',
                what.block, 
                'STREAMS', 
                what.stream, 
                what.id
            ) as Consumption[] | null;
        } else {
            consumption = await this.redis.xread(
                'COUNT', 
                what.count, 
                'BLOCK',
                what.block, 
                'STREAMS',
                what.stream, 
                what.id
            ) as Consumption[] | null;
        }

        const consumeItems: ConsumeItem[] = [];
        const items: Consumption[] = consumption ?? [];

        for (const item of items) {
            const [stream, messages] = item as [string, Consumption];

            for (const message of messages) {
                const [id, payload] = message as [string, string[] | null];

                const entries = (payload ?? [] as string[]).flatMap((_, i, a) => {
                    return i % 2 ? [] : [a.slice(i, i + 2)];
                });
                
                consumeItems.push({
                    stream,
                    id,
                    payload: Object.fromEntries([...entries])
                });
            }   
        }

        return consumeItems;
    }
}
