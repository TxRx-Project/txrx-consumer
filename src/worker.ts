import { Consumable, ConsumeItem } from "../types/consumer.types";
import Consumer from "./consumer";
import { setTimeout } from 'timers/promises';

export default abstract class Worker {
    public abstract startPel(): string;
    public abstract consumable(): Consumable;
    public abstract consume(item: ConsumeItem): Promise<boolean>;

    private consumer: Consumer;
    private running = true;
    private consuming: Consumable;
    private nextPel = false;

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

    public consumePel() {
        this.nextPel = true;
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

            if (this.consuming.id !== '>') {
                await setTimeout(this.consuming.block);

                const last = items.pop();
                this.consuming.id = last?.id ?? '>';
            }

            if (this.nextPel) {
                this.nextPel = false;
                this.consuming.id = this.startPel();
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
