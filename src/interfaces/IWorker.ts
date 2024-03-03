import { Consumable, ConsumeItem } from "../../types/consumer.types";

/**
 * The worker interface.
 */
export interface IWorker {
    /**
     * Dicatates the worker consuming behavior.
     * 
     * @returns a {@link Consumable}
     */
    consumable(): Consumable;

    /**
     * Dictates how the worker shall consume items from the stream.
     * 
     * The resolving boolean hints the worker to acknowledge the item.
     * 
     * @returns a Promise which resolves into a boolean
     */
    consume(item: ConsumeItem): Promise<boolean>;
}

export const IWorker = Symbol('IWorker');
