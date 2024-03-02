import { Consumable, ConsumeItem, ConsumingMode } from "../types/consumer.types";
import Consumer from "./consumer";

export default abstract class Worker {
    public abstract startPel(): string;
    public abstract consumable(): Consumable;
    public abstract consume(item: ConsumeItem): Promise<boolean>;

    private consumer: Consumer;
    private running = true;
    private consuming: Consumable;

    constructor(url: string) {
        this.consumer = new Consumer(url);
    }

    public isRunning(): boolean {
        return this.running;
    }

    public setRunning(running: boolean) {
        this.running = running;
    }

    public getConsuming(): Consumable {
        return this.consuming;
    }

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
                this.consuming.id = last?.id ?? '0';
                this.consuming.mode = ConsumingMode.NORMAL;
            } else {
                this.consuming.mode = ConsumingMode.PEL;
            }
        }
    }

    protected async consumption(items: ConsumeItem[]) {
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
