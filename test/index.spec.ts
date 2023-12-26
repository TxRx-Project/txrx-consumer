import * as Index from '../index';
import Consumer from '../src/consumer';
import Worker from '../src/worker';

test('index exports', () => {
    expect(Index.Worker).toBe(Worker);
    expect(Index.Consumer).toBe(Consumer);
});

test('index scope', () => {
    expect(Object.keys(Index).sort()).toEqual([
        'Consumer',
        'Worker',
    ].sort())
});
