import redisMock from 'ioredis-mock';
import DummyWorker from '../worker/dummyWorker';
import { Consumable, Consumption } from '../../types/consumer.types';

let timeoutArgs: any = [];

jest.mock('ioredis', () => jest.requireActual('ioredis-mock'));
jest.mock('timers/promises', () => {
    return {
        setTimeout: async (...args: any) => {
            timeoutArgs.push(args);
        },
    }
});

let redisArgs: any[] = [];
let xgroupResolve = 0;
let xgroupException;

redisMock.prototype.xgroup = jest.fn().mockImplementation(async (...args: any) => {
    redisArgs.push('XGROUP', args);

    if (xgroupException) {
        throw xgroupException;
    }

    return xgroupResolve;
});

let xackResolve = 0;

redisMock.prototype.xack = jest.fn().mockImplementation(async (...args: any) => {
    redisArgs.push('XACK', args);
    return xackResolve;
});

let streamItems: Consumption[][] | null;

redisMock.prototype.xreadgroup = jest.fn().mockImplementation(async (...args: any) => {
    redisArgs.push('XREADGROUP', args);

    const batch = streamItems?.shift();

    if (batch) { 
        return [[
            'TEST:STREAM', batch
        ]];
    }

    return null;
});

let infoArgs: any[] = [];

jest.spyOn(console, 'info').mockImplementation((...args: any) => {
    infoArgs.push(args);
});

let errorArgs: any[] = [];

jest.spyOn(console, 'error').mockImplementation((...args: any) => {
    errorArgs.push(args);
});

describe('The Worker class', () => {
    const worker = new DummyWorker('redis://localhost:6379');
    const now = Date.now();
    const id1 = now + 1 + '';
    const id2 = now + 2 + '';
    const id3 = now + 3 + '';
    const id4 = now + 4 + '';
    const id5 = now + 5 + '';
    const id6 = now + 6 + '';
    const id7 = now + 7 + '';
    const id8 = now + 8 + '';
    let consuming: Consumable;

    beforeEach(() => {
        redisArgs = [];
        streamItems = null;
        xgroupResolve = 0;
        xackResolve = 0;
        infoArgs = [];
        errorArgs = [];
        xgroupException = null;
        timeoutArgs = [];
        worker.stopsAt = 1;
        worker.consumptions = 0;
    });

    it('can tell if running', () => {
        expect(worker.isRunning()).toEqual(true);
        worker.setRunning(false);
        expect(worker.isRunning()).toEqual(false);
        worker.setRunning(true);
        expect(worker.isRunning()).toEqual(true);
    });

    it('create group even if not running', async () => {
        worker.setRunning(false);

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP', 
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
        ])
    });

    it('handles the busy group exception on dev environments', async () => {
        process.env.NODE_ENV = 'development';
        worker.setRunning(false);

        xgroupException = 'it is a BUSYGROUP error';

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP',
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
        ]);

        expect(infoArgs).toEqual([
            [xgroupException]
        ]);
    });

    it('handles the busy group exception on non dev environments', async () => {
        process.env.NODE_ENV = 'production';
        worker.setRunning(false);

        xgroupException = 'BUSYGROUP';

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP',
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
        ]);

        expect(infoArgs).toEqual([]);
    });

    it('process pending message (PEL mode)', async () => {
        streamItems = [
            [
                [id1, null], 
                [id2, ['test', '2', 'foo', 'bar']], 
            ], [
                [id3, ['test', '3', 'foo', 'baz']], 
            ], [
                [id4, ['test', '4', 'foo', 'bat']], 
                [id5, ['test', '5', 'foo', 'bax']],  
            ], [
                [id6, ['test', '4', 'foo', 'bat']], 
                [id7, ['test', '5', 'foo', 'bax']],  
                [id8, ['test', '5', 'foo', 'bax']],  
            ]
        ];

        worker.stopsAt = streamItems.length + 2;

        worker.setRunning(true);

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP', 
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '$' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id2 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id2 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id3 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id3 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id4 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id5 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id5 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id6 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id7 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id8 ],            
            'XREADGROUP',
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id8 ],  
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
        ]);

        consuming = worker.getConsuming();

        expect(consuming).toEqual({
            count: 100,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>',
        });

        expect(timeoutArgs).toEqual([
            [2000],
            [2000],
            [2000],
            [2000],
            [2000],
        ]);
    });

    it('it process message in the normal mode (stuck to the > special ID)', async () => {
        streamItems = [
            [
                [id1, ['test', '1', 'foo', 'bar']], 
                [id2, ['test', '2', 'foo', 'bar']], 
            ], [
                [id3, null], 
            ], [
                [id4, ['test', '4', 'foo', 'bat']], 
                [id5, ['test', '5', 'foo', 'bax']],  
            ], [
                [id6, ['throw', 'error']], 
                [id7, null],  
                [id8, ['test', '5', 'foo', 'bax']],  
            ]
        ];

        worker.stopsAt = streamItems.length + 2;

        worker.setRunning(true);

        worker.consumingOverride = {
            count: 100,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>',
        };

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP', 
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id1 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id2 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id4 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id5 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id8 ],            
            'XREADGROUP',
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],  
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
        ]);

        consuming = worker.getConsuming();

        expect(consuming).toEqual({
            count: 100,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>',
        });

        expect(timeoutArgs).toEqual([]);
        expect(errorArgs).toEqual([
            ['error']
        ]);
    });

    it('can switch to the PEL mode on demand', async () => {
        streamItems = [
            [
                [id1, ['test', '1', 'foo', 'bar']], 
                [id2, ['test', '2', 'foo', 'bar']], 
            ], [
                [id3, ['pel', '0-0']], 
            ], [
                [id4, ['test', '4', 'foo', 'bat']], 
                [id5, ['test', '5', 'foo', 'bax']],  
            ], [
                [id6, ['test', '6', 'foo', 'bax']],
                [id7, ['test', '7', 'foo', 'bax']],
                [id8, ['test', '8', 'foo', 'bax']],  
            ]
        ];

        worker.stopsAt = streamItems.length + 2;

        worker.setRunning(true);

        worker.consumingOverride = {
            count: 100,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>',
        };

        await worker.run();

        expect(redisArgs).toEqual([
            'XGROUP', 
            [ 'CREATE', 'TEST:STREAM', 'TEST:GROUP', '$', 'MKSTREAM' ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id1 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id2 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id3 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '0-0' ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id4 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id5 ],
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id5 ],
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id6 ],   
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id7 ],   
            'XACK',
            [ 'TEST:STREAM', 'TEST:GROUP', id8 ],            
            'XREADGROUP',
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', id8 ],  
            'XREADGROUP', 
            [ 'GROUP', 'TEST:GROUP', 'TEST:CONSUMER:0', 'COUNT', 100, 'BLOCK', 2000, 'STREAMS', 'TEST:STREAM', '>' ],
        ]);

        consuming = worker.getConsuming();

        expect(consuming).toEqual({
            count: 100,
            block: 2000,
            stream: 'TEST:STREAM',
            group: 'TEST:GROUP',
            consumer: 'TEST:CONSUMER:0',
            id: '>',
        });

        expect(timeoutArgs).toEqual([
            [2000],
            [2000],
            [2000],
        ]);
    });
});