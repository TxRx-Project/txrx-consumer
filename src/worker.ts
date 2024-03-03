import { Consumable, ConsumeItem, ConsumingMode } from "../types/consumer.types";
import Consumer from "./consumer";
import { IWorker } from "./interfaces/IWorker";

/**
 * The worker class defines a blueprint for a Redis streaming consumer service.
 * 
 * The consuming is entirely dicated by the {@link Consumable} provided by its implementation.
 */
export default abstract class Worker implements IWorker {
    public abstract consumable(): Consumable;
    public abstract consume(item: ConsumeItem): Promise<boolean>;

    private consumer: Consumer;
    private running = true;
    private consuming: Consumable;

    /**
     * The worker class constructor.
     * 
     * @param url - Redis connection string
     */
    constructor(url: string) {
        this.consumer = new Consumer(url);
    }

    /**
     * Tells the running state of the worker.
     * 
     * @returns a boolean
     */
    public isRunning(): boolean {
        return this.running;
    }

    /**
     * Updates the running state of the worker.
     * 
     * @param running - a boolean for the new running state of the worker
     */
    public setRunning(running: boolean) {
        this.running = running;
    }

    /**
     * Accessor for the current {@link Consumable}
     * 
     * @returns the current {@link Consumable}
     */
    public getConsuming(): Consumable {
        return this.consuming;
    }

    /**
     * The worker entrypoint.
     * 
     * - Creates the consuming group and stream
     * 
     * - Consumes the stream as dictated by the current {@link Consumable} 
     * 
     * - In PEL mode, the worker automatically switches to NORMAL on empty responses
     * 
     * Note the consumer's call is not try/catch wrapped on purpose, the idea
     * is to halt the service immediately on a XREAD/XREADGROUP error.
     * 
     * @returns a Promise which resolves to void
     */
    public async run(): Promise<void> {
        this.consuming = this.consumable();

        try {
            if (this.consuming.consumer && this.consuming.group) {
                await this.consumer.createGroup(this.consuming);
            }
        } catch (e) {
            if (/BUSYGROUP/.test(e)) {
                if (process.env.NODE_ENV! === 'development') {
                    console.info(e);
                }
            }
        }

        while (this.running) {
            const items: ConsumeItem[] = await this.consumer.consume(this.consuming);

            await this.consumption(items);

            if (this.consuming.mode === ConsumingMode.PEL) {
                const last = items.pop();
                this.consuming.id = last?.id ?? '>';
            }

            if (this.consuming.id === '>') {
                this.consuming.mode = ConsumingMode.NORMAL;
            } else {
                this.consuming.mode = ConsumingMode.PEL;
            }
        }
    }

    /**
     * Consumes the collection of items gathered from the stream.
     * 
     * @param items - an array of {@link ConsumeItem}
     * @returns a Promise which resolves to void
     */
    protected async consumption(items: ConsumeItem[]): Promise<void> {
        await Promise.all(items.map(async (item) => {
            try {
                const ack = await this.consume(item);

                if (ack) {
                    await this.consumer.ack(this.consuming, item);
                }
            } catch (e) {
                console.error(e);
            }
        }));
    }
}
