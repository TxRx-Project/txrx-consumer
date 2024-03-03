import { Consumable, ConsumeItem, ConsumingMode, Consumption } from './../types/consumer.types';
import { RedisConnector } from "@txrx/redis-pool";
import Redis from 'ioredis';

/**
 * A Redis stream consumer implementation.
 * 
 * Support the following Redis commands:
 * 
 * - XREADGROUP
 * 
 * - XREAD
 * 
 * - XACK
 *
 * The behavior of this class is entirely dictated by the {@link Consumable} type.
 */
export default class Consumer {
    /**
     * The internal {@link Redis} instance.
     */
    private redis: Redis;

    /**
     * The Consumer constructor.
     *
     * @param url - a Redis connection string
     */
    constructor(url: string) {
        this.redis = RedisConnector.get('consumer').get(url);
    }

    /**
     * Creates a Redis consuming group and stream if needed.
     * 
     * @param what - a {@link Consumable} that specifies the stream and group
     * @returns a Promise which resolves into the result of the XGROUP CREATE issued
     */
    public async createGroup(what: Consumable): Promise<unknown> {
        return this.redis.xgroup('CREATE', what.stream, what.group, '$', 'MKSTREAM');
    }

    /**
     * Acknowledges a stream item.
     * 
     * Perform the XACK over the resolved item ids. If a {@link ConsumeItem} is provided, the
     * XACK will target its id. If an array of strings is provided, the XACK call will utilize
     * the whole array as the ids argument.
     *
     * @param what - a {@link Consumable} that specifies the stream and group
     * @param which - either a {@link ConsumeItem} or array of strings
     * @returns a Promise which resolves into the number of messages succefully acknowledged
     */
    public async ack(what: Consumable, which: ConsumeItem | string[]): Promise<number> {
        if (Object.hasOwn(which,'id')) {
            const w = which as ConsumeItem;
            return this.redis.xack(w.stream, what.group, w.id);
        }

        return this.redis.xack(what.stream, what.group, ...(which as string[]));
    }

    /**
     * Performs the actual stream consumption.
     * 
     * It support two modes:
     * 
     * - PEL: consumes the consumer's pending entry list from the specified id
     * 
     * - NORMAL: the special \> is used to consume new messages only
     * 
     * It also supports two stream consuming commands:
     * 
     * - XREAD: no consuming group involved
     * 
     * - XREADGROUP only if the group and consumer names are defined within the {@link Consumable}
     *
     * @param what - a {@link Consumable} which dictates the consumption
     * @returns a Promise which resolves into an array of {@link ConsumeItem}
     */
    public async consume(what: Consumable): Promise<ConsumeItem[]> {
        let consumption: Consumption[] | null;
        let id = what.id;

        if (what.mode === ConsumingMode.NORMAL) {
            id = '>';
        }

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
                id
            ) as Consumption[] | null;
        } else {
            consumption = await this.redis.xread(
                'COUNT', 
                what.count, 
                'BLOCK',
                what.block, 
                'STREAMS',
                what.stream, 
                id
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
