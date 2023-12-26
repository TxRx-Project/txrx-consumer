export type Consumable = {
    group?: string;
    consumer?: string;
    count: number;
    block: number;
    stream: string;
    id: string;
};

export type Consumption = [
    string,
    Consumption[] | string[] | null
];

export type Payload = {
    [key:string]: string;
};

export type ConsumeItem = {
    stream: string,
    id: string,
    payload: Payload,
};
