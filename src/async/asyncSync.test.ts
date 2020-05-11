import { toSelector } from "../selector";
import {  delay } from "simple-pure-utils";
import { createSelectorAsync } from "./async";
import * as rx from "rxjs";
import { syncResolve } from "simple-pure-utils";

test("observable de promesa inmediata es inmediato", () => {
    const prom = syncResolve(10);
    const obs = rx.from(prom);
    let result = 0;
    obs.subscribe(x => result = x);

    //Se resuelve de inmediato:
    expect(result).toBe(10);
});

test("selector promesa inmediata debe de resolverse de inmediato", async () => {
    {
        //Selector que devuelve una promesa NO inmediata:
        const sel = toSelector(() => Promise.resolve(10));

        const sum1 = createSelectorAsync({ sel }, x => x.sel + 1);

        let result = 0;
        const promise = sum1.call({});
        promise.then(x => result = x);
        //La promesa aún no esta resuelta en este punto:
        expect(result).toBe(0);
        await promise;

        //Ya se resolvió
        expect(result).toBe(11);
    }

    //Hacer el mismo experimento pero con el sync resolve:
    {
        //Selector que devuelve una promesa NO inmediata:
        const sel = toSelector(() => syncResolve(10));

        const sum1 = createSelectorAsync({ sel }, x => x.sel + 1);

        let result = 0;
        const promise = sum1.call({});
        promise.then(x => result = x);
        //La promesa SI se resolvió de inmediato
        expect(result).toBe(11);
        await promise;

        //El valor sigue igual:
        expect(result).toBe(11);
    }
});