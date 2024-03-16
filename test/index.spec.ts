import * as Index from '../index';
import Consumer from '../src/consumer';
import Worker from '../src/worker';
import * as Types from '../types/consumer.types';

test('index exports', () => {
    expect(Index.Worker).toBe(Worker);
    expect(Index.Consumer).toBe(Consumer);
    expect(Index.Consumable).toBe(Types.Consumable);
    expect(Index.Consumption).toBe(Types.Consumption);
    expect(Index.Payload).toBe(Types.Payload);
    expect(Index.ConsumeItem).toBe(Types.ConsumeItem);
});

test('index scope', () => {
    expect(Object.keys(Index).sort()).toEqual([
        'Consumer',
        'Worker',
        'Consumable',
        'Consumption',
        'Payload',
        'ConsumeItem',
    ].sort())
});
