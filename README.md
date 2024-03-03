# txrx-redis-consumer

A blueprint for `Redis` streaming consumers.

## Synopsis

An abstract `Worker` class is provided which must comply to the `IWorker` interface.

```typescript
class MyWorker extends Worker implements IWorker {
    ...
}
```

The `IWorker` denotes two important things:

- What to consume, also known as the `Consumable`.
- How to consume the gathered items.

### Consumable

This type defines the following properties:

- **consumer** - name of the consumer
- **group** - name of the consuming group
- **stream** - name of the stream to consume
- **count** - items to gather per read
- **block** - time to block and wait for messages
- **id** - the ID from where to start consuming in PEL mode, for new messages only the special `>` id is used

### ConsumeItem

This type defines the items to be consumed from the stream. Once gathered, the worker will parse the intricate structure into this rather simple structureL

- **stream** - the name of the stream this item came from
- **id** - id of the item within its stream
- **payload** - a simple string to string mapping, containing all the data attached to the message.

***Note: sometimes the payload might be empty, is on the implementation side to deal with edge cases, e.g: the element has been removed from the stream while also being part of the consumer's PEL. In this scenario you would get an empty payload.***

### Usage

```typescript
const worker = new MyWorker();

(async() {
    await worker.run();
})();
```

***Note: the above is just a minimalistic example***

## Devel

Dev container is recommended, to run the `devel` container:

```bash
make build
make install
```

### CI

The workflow runs:

```bash
make test
```

Or separately:

#### Tests

```bash
make jest
```

#### Linter

```bash
make syntax
```

