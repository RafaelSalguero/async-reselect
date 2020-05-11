import { toSelector } from "../selector";
import { createSelectorAsync, createSelectorRx } from "./async";
import { delay, obsToPromise, toObservable, rxFlatten } from "simple-pure-utils";
import { toSyncPromise, isSyncPromise } from "simple-pure-utils";
import { Observable, from } from "rxjs";
import { map } from "rxjs/operators";

test("obsToPrimise", async () => {
    //La función es asíncrona y devuelve 20
    const func = async (id: number) => {
        delay(10);
        return 20;
    };

    const prom = toSyncPromise(func(0));


    const obs = toObservable(prom);
    const fromObs = obsToPromise(obs);

    const value = await prom;
    expect(isSyncPromise(prom)).toBe(true);
    expect(isSyncPromise(fromObs)).toBe(true);
})

test("promise resolved is sync", async () => {
    //La función es asíncrona y devuelve 20
    const func = async (id: number) => {
        delay(10);
        return 20;
    };

    const idSel = toSelector((id: number) => id);
    const funcSel = createSelectorAsync({ idSel }, s => func(s.idSel));

    const prom1 = funcSel.call(0);
    //La primera llamada es asíncrona:
    expect(isSyncPromise(prom1)).toBe(false);

    const value = await prom1;
    expect(value).toBe(20);

    const prom2 = funcSel.call(0);
    //La segunda llamada es síncrona:
    expect(isSyncPromise(prom2)).toBe(true);
});


test("observable same instance", async () => {
    //La función es asíncrona y devuelve 20
    const func = async (id: number) => {
        delay(10);
        return 20;
    };

    const getValueFunc = () => "hola";
    const getValue = toSelector((id: number) => getValueFunc());

    const mapFunc = createSelectorRx({ getValue }, s => (x) => {
        return x + 1;
    });

    let test: any[] = [];
    const itemsMapped = createSelectorRx({ mapFunc }, (s, old) => {
        test.push(old);
        return from([1]).pipe(map(s.mapFunc));
    });

    const b1 = itemsMapped.call(1);
    const b2 = itemsMapped.call(1);

    expect(b1).toBe(b2);

    debugger;
    b1.subscribe(x => {

    });
});


