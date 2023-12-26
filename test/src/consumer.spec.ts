import redisMock from 'ioredis-mock';
import Consumer from '../../src/consumer';
import { Consumable, ConsumeItem, Consumption } from '../../types/consumer.types';

jest.mock('ioredis', () => jest.requireActual('ioredis-mock'));

let xgroupArgs = [];
let xgroupResolve = 0;

redisMock.prototype.xgroup = jest.fn().mockImplementation(async (...args: any) => {
    xgroupArgs = args;
    return xgroupResolve;
});

let xackArgs = [];
let xackResolve = 0;

redisMock.prototype.xack = jest.fn().mockImplementation(async (...args: any) => {
    xackArgs = args;
    return xackResolve;
});

let xreadgroupArgs = [];
let streamItems: Consumption[] | null = null;

redisMock.prototype.xreadgroup = jest.fn().mockImplementation(async (...args: any) => {
    xreadgroupArgs = args;
    return streamItems;
});

describe('The Consumer class', () => {
    const consumer = new Consumer('redis://localhost:6379');
    
    it('can create a consuming group', async() => {
        const consumable: Consumable = {
            count: 1,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            id: '>'
        };

        xgroupResolve = 1;
        xgroupArgs = [];

        expect(consumer.createGroup(consumable)).resolves.toEqual(1);
        expect(xgroupArgs).toEqual([
            'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM'
        ]);
    });

    it('can acknowledge a consume item', async () => {
        const id = Date.now() + '-0';
        const consumable: Consumable = {
            count: 1,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>'
        };

        const item: ConsumeItem = {
            stream: consumable.stream,
            payload: {
                test: '1',
            },
            id,
        };

        xackResolve = 1;
        xackArgs = [];

        expect(consumer.ack(consumable, item)).resolves.toEqual(1);
        expect(xackArgs).toEqual([
            'TEST:STREAM', 'TEST:GROUP', id,
        ]);
    });
    
    it('can acknowledge a list of message ids', async () => {
        const id1 = Date.now() + '-0';
        const id2 = Date.now() + '-1';
        const id3 = Date.now() + '-2';
        const consumable: Consumable = {
            count: 1,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>'
        };

        const ids = [
            id1,
            id2,
            id3,
        ];

        xackResolve = 1;
        xackArgs = [];

        expect(consumer.ack(consumable, ids)).resolves.toEqual(1);
        expect(xackArgs).toEqual([
            'TEST:STREAM', 'TEST:GROUP', ...ids
        ]);
    });
    
    it('can consume stream messages, in a consuming group', async () => {
        const consumable: Consumable = {
            count: 1,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>'
        };
    
        xreadgroupArgs = [];
        streamItems = null;

        expect(consumer.consume(consumable)).resolves.toEqual([]);
        expect(xreadgroupArgs).toEqual([
            'GROUP', 
            'TEST:GROUP',
            'TEST:CONSUMER:0',
            'COUNT', 
            1, 
            'BLOCK',
            2000, 
            'STREAMS',
            'TEST:STREAM', 
            '>',
        ]);

        const now = Date.now();
        const id1 = now + 1 + '';
        const id2 = now + 2 + '';
        const id3 = now + 3 + '';

        xreadgroupArgs = [];
        streamItems = [[
            'TEST:STREAM', [
                [id1, ['test', '1', 'foo', 'bar']], 
            ],
        ]];

        expect(consumer.consume(consumable)).resolves.toEqual([{
            stream: consumable.stream,
            id: id1,
            payload: {
                test: '1',
                foo: 'bar',
            },
        }]);

        expect(xreadgroupArgs).toEqual([
            'GROUP', 
            'TEST:GROUP',
            'TEST:CONSUMER:0',
            'COUNT', 
            1, 
            'BLOCK',
            2000, 
            'STREAMS',
            'TEST:STREAM', 
            '>',
        ]);

        xreadgroupArgs = [];
        streamItems = [[
            'TEST:STREAM', [
                [id2, null], 
                [id3, ['test', '3', 'foo', 'baz']], 
            ],
        ]];

        expect(consumer.consume(consumable)).resolves.toEqual([{
            stream: consumable.stream,
            id: id2,
            payload: {},
        }, {
            stream: consumable.stream,
            id: id3,
            payload: {
                test: '3',
                foo: 'baz',
            },
        }]);
        
        expect(xreadgroupArgs).toEqual([
            'GROUP', 
            'TEST:GROUP',
            'TEST:CONSUMER:0',
            'COUNT', 
            1, 
            'BLOCK',
            2000, 
            'STREAMS',
            'TEST:STREAM', 
            '>',
        ]);
    });

    it('can consume stream messages, not in a consuming group', async () => {
        const consumable: Consumable = {
            count: 1,
            block: 1,
            stream: 'TEST:STREAM',
            id: '$'
        };
        
        expect(consumer.consume(consumable)).resolves.toEqual([]);
    });
});