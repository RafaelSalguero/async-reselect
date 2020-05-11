import * as rx from "rxjs";
import { toSelector, createSelector } from "../selector";
import { createSelectorRx, createSelectorAsync } from "./async";
import * as rxOps from "rxjs/operators";
import { delay, isObservable, toObservable, deepEquals } from "simple-pure-utils";

async function expectObs<T>(actual: rx.Observable<T>, expected: T[]) {
    const arr = await actual.pipe(rxOps.toArray()).toPromise();
    expect(arr).toEqual(expected);
}

test("selector mismo observable", async () => {
    const selId = toSelector((id: number) => id);
    const selValue = toSelector((id: number) => id * 2);

    const obs = rx.from([3]);
    const obsSel = toSelector((id: number) => obs);

    const mul = createSelectorRx({
        selValue,
        obsSel
    }, x => x.selValue * x.obsSel);

    {
        const x = mul.call(4);
        const y = mul.call(4);
        const z = mul.call(3);

        await expectObs(x, [24]);
        expect(x).toBe(y);

        await expectObs(z, [18]);
        expect(z).not.toBe(x);

    }
});


test("selector devolver el mismo observable 2", async () => {
    type In = { props: number, state: number | null };
    const id = toSelector((x: In) => x.props);


    const valueFromState = toSelector((x: In) => x.state);
    let subCount = 0;
    const valueFromResource = createSelectorRx({ id }, x => new rx.Observable<number>(subs => {
        subs.next(x.id * 3);
        subCount++;
    }));

    const value = createSelector({
        state: valueFromState,
        resource: valueFromResource
    }, x => x.state ? rx.from([x.state]) : x.resource);

    const obs0 = value.call({ props: 10, state: null });
    obs0.subscribe(x => { });

    const obs1A = value.call({ props: 10, state: 2 });
    obs1A.subscribe(x => { });

    const obs1B = value.call({ props: 10, state: 2 });
    obs1B.subscribe(x => { });

    const obs2 = value.call({ props: 10, state: 3 });
    obs2.subscribe(x => { });

    const obs3 = value.call({ props: 10, state: 4 });
    obs3.subscribe(x => { });

    expect(subCount).toBe(1);

    expect(obs0).not.toBe(obs1A);

    expect(obs1A).toBe(obs1B);
    expect(obs1A).not.toBe(obs2);
    expect(obs2).not.toBe(obs3);

});

test("async create selector simple test", async () => {
    interface Props {
        value: number;
    }
    const value = toSelector((x: Props) => x.value);

    const incrementAsync = createSelectorRx({ value }, x => delay(100).then(() => x.value + 1));

    const dup = createSelectorRx({ value }, x => x.value * 2);
    const sum = createSelectorRx({ incrementAsync, dup }, x => x.incrementAsync + x.dup);

    const func = (x: number) => (x + 1) + (x * 2);
    //(value + 1) + (value * 2)
    expect(await sum.call({ value: 2 }).toPromise()).toBe(func(2));
    expect(await sum.call({ value: 5 }).toPromise()).toBe(func(5));
});

test("async create selector async memoize", async () => {
    interface Props {
        a: number;
        b: number;
    }
    const a = toSelector((x: Props) => x.a);
    const increment = createSelectorAsync({ a }, x => delay(100).then(() => x.a + 1));
    let samePromiseCalls = 0;
    const samePromise = createSelectorAsync({ increment }, x => {
        samePromiseCalls++;
        return Promise.resolve(10);
    }); //Este selector cada vez devuelve una promesa diferente, pero con el mismo valor

    let sumCalls = 0;
    const sumObs = createSelectorAsync({ samePromise }, x => {
        sumCalls++;
        return x.samePromise + 1;
    });

    const sum = (x: Props) => sumObs.call(x);

    expect(samePromiseCalls).toBe(0);
    expect(await sum({ a: 10, b: 20 })).toBe(11);

    expect(samePromiseCalls).toBe(1);
    expect(sumCalls).toBe(1);

    expect(await sum({ a: 10, b: 25 })).toBe(11);

    //El cambio de B no ocasionó llamadas
    expect(samePromiseCalls).toBe(1);
    expect(sumCalls).toBe(1);

    expect(await sum({ a: 15, b: 25 })).toBe(11);

    //El cambio de A ocasiono una llamada a samePromise ya que depende de increment, el cual depende de A
    expect(samePromiseCalls).toBe(2);
    //sumCalls no se llamo ya que la promesa aunque diferente, devolvió lo mismo que es 10
    expect(sumCalls).toBe(1);

    expect(await sum({ a: 15, b: 25 })).toBe(11);

    //No cambiaron los valores, no hay ninguna llamada:
    expect(samePromiseCalls).toBe(2);
    expect(sumCalls).toBe(1);

    expect(await sum({ a: 10, b: 25 })).toBe(11);

    expect(samePromiseCalls).toBe(3);
    expect(sumCalls).toBe(1);
});


test("async same promise ", async () => {
    interface Props {
        a: number;
        b: number;
    }
    const props = toSelector((x: Props) => x);
    let samePromiseCalls = 0;
    const samePromise = createSelectorRx({ props }, x => {
        samePromiseCalls++;
        return Promise.resolve({ a: 10, b: 30 });
    }); //Este selector cada vez devuelve una promesa diferente, pero con el mismo valor

    let sumCalls = 0;
    const sum = createSelectorRx({ samePromise }, x => {
        sumCalls++;
        return x.samePromise.a + 1;
    }, { comparer: deepEquals });

    expect(samePromiseCalls).toBe(0);
    expect(sumCalls).toBe(0);

    await sum.call({ a: 10, b: 20 }).toPromise();

    expect(samePromiseCalls).toBe(1);
    expect(sumCalls).toBe(1);

    await sum.call({ a: 10, b: 20 }).toPromise();

    expect(samePromiseCalls).toBe(2);
    expect(sumCalls).toBe(1);
});


test("selector al tener error no debe de memoizar SYNC", async () => {
    const a = toSelector(() => 10);
    let count = 0;
    const sum = createSelector({ a }, x => {
        count++;
        if (count >= 3) return x.a;
        else
            throw new Error("Error de prueba");
    });

    let errCount = 0;
    try { sum.call({}); } catch (error) { errCount++; }
    try { sum.call({}); } catch (error) { errCount++; }
    expect(sum.call({})).toBe(10);

    expect(errCount).toBe(2);
    expect(count).toBe(3);

    sum.call({});
    sum.call({});

    expect(errCount).toBe(2);
    expect(count).toBe(3);
});



test("selector con observable", async () => {
    interface Props {
        a: number;
    };
    const a = toSelector((x: Props) => x.a);
    let count = 0;
    const contarA = createSelectorRx({ a }, a => {
        count++;
        return rx.timer(0, 100).pipe(rxOps.takeWhile(x => x < a.a));
    });
    const conteoPor2 = createSelectorRx({ contarA }, a => a.contarA * 2);

    let obs2 = await conteoPor2.call({ a: 2 }).pipe(rxOps.toArray()).toPromise();
    expect(obs2).toEqual([0, 2]);

    obs2 = await conteoPor2.call({ a: 2 }).pipe(rxOps.toArray()).toPromise();
    expect(obs2).toEqual([0, 2]);
    expect(count).toBe(1);

    let obs4 = await conteoPor2.call({ a: 4 }).pipe(rxOps.toArray()).toPromise();
    expect(obs4).toEqual([0, 2, 4, 6]);

    obs4 = await conteoPor2.call({ a: 4 }).pipe(rxOps.toArray()).toPromise();
    expect(obs4).toEqual([0, 2, 4, 6]);

    expect(count).toBe(2);

});


test("selecotr con observable problema TEST", async () => {
    interface State {
    }

    const idCliente = toSelector((state: State) => new rx.BehaviorSubject<number>(10));

    const idClienteObs = idCliente.call({});
    const idClienteValue = await idClienteObs.pipe(rxOps.first(), rxOps.toArray()).toPromise();
    expect(isObservable(idClienteObs)).toBe(true);
    expect(idClienteValue).toEqual([10]);

    const cliente = createSelectorRx({ idCliente }, id => {
        expect(id.idCliente).toBe(10);
        var b = new rx.BehaviorSubject<{ Direcciones: any[] }>({ Direcciones: [1, 2, 3] } as any);
        const isObs = isObservable(b);
        return b;
    });


    const clienteObs = cliente.call({});
    const clienteResult = await clienteObs.pipe(rxOps.first(), rxOps.toArray()).toPromise();

    expect(clienteResult).toEqual([{ Direcciones: [1, 2, 3] }]);

}, 10000);


test("selector debe de devolver la misma instancia de observable argumento sincrono", async () => {
    interface State {
    }

    const idCliente = toSelector((state: State) => 10);
    let calls = 0;
    const cliente = createSelectorRx({ idCliente }, id => {
        calls++;
        const instance = { id: id.idCliente };
        return rx.from([instance]);
    });

    const state: State = {};
    const miCliente1 = cliente.call(state);
    const miCliente2 = cliente.call(state);

    //No hay ninguna llamada por que no hay subscripciones:
    expect(calls).toBe(0);

    //Es el mismo observable:
    expect(miCliente1).toBe(miCliente2);

    const miClienteResult1 = await miCliente1.pipe(rxOps.first(), rxOps.toArray()).toPromise();
    const miClienteResult2 = await miCliente2.pipe(rxOps.first(), rxOps.toArray()).toPromise();

    //Después de las subscripciones sólo hay 1 llamada, no 2:
    expect(calls).toBe(1);


    expect(miClienteResult1[0]).toBe(miClienteResult2[0]);
});


test("selector debe de devolver la misma instancia de observable con argumento observable", async () => {
    interface State {
    }

    const idCliente = toSelector((state: State) => 10);
    const idClienteObs = createSelectorRx({ idCliente }, id => new rx.BehaviorSubject<number>(id.idCliente));

    let calls = 0;
    const cliente = createSelectorRx({ idClienteObs }, id => {
        calls++;
        const instance = { id: id.idClienteObs };
        return rx.from([instance]);
    });


    const state: State = {};
    const miCliente1 = cliente.call(state);
    const miCliente2 = cliente.call(state);

    const miClienteResult1 = await miCliente1.pipe(rxOps.first(), rxOps.toArray()).toPromise();
    const miClienteResult2 = await miCliente2.pipe(rxOps.first(), rxOps.toArray()).toPromise();

    expect(calls).toBe(1);
    expect(miClienteResult1[0]).toBe(miClienteResult2[0]);
    expect(miCliente1).toBe(miCliente2);
});



test("selector debe de devolver la misma instancia de promesa argumento sincrono", async () => {
    interface State {
    }

    const idCliente = toSelector((state: State) => 10);
    let calls = 0;
    const cliente = createSelectorAsync({ idCliente }, id => {
        calls++;
        const instance = { id: id };
        return Promise.resolve(instance);
    });

    const state: State = {};
    const miCliente1 = cliente.call(state);
    const miCliente2 = cliente.call(state);

    expect(calls).toBe(1);
    expect(miCliente1).toBe(miCliente2);

    const miClienteResult1 = await miCliente1;
    const miClienteResult2 = await miCliente2;
    expect(miClienteResult1).toBe(miClienteResult2);
});


test("selecotr con observable problema COMPLETA", async () => {

    interface State {
        idCliente?: number;
    }
    interface Props {
        value?: number | null;
        loading?: boolean;
    }
    interface In {
        state: State;
        props: Props;
    }
    const idClienteDireccion = toSelector((x: In) => x.props.value);
    const clienteDireccion = createSelectorRx({ idClienteDireccion }, id => {
        //Hello
        var b = new rx.BehaviorSubject<{ Cliente: number, IdCliente: number }>({ Cliente: 10, IdCliente: 1 } as any);
        const ret = id != null ? b : rx.from([id]);
        return ret as rx.Observable<{ Cliente: number, IdCliente: number } | null | undefined>;
    });
    const clienteFromValue = createSelectorRx({ clienteDireccion }, x => x.clienteDireccion && x.clienteDireccion.Cliente);
    const idClienteFromValue = createSelectorRx({ clienteDireccion }, x => x.clienteDireccion && x.clienteDireccion.IdCliente);
    const idClienteFromState = toSelector((x: In) => x.state.idCliente);
    //NOTA: Le damos prioridad al NULL sobre el UNDEFINED, esto por que un NULL indica una limpieza por parte del usuario, en cambio un undefined indica que el valor no se conoce
    const idCliente = createSelectorRx({ val: idClienteFromValue, state: idClienteFromState }, x => x.val === null ? null : (x.state || x.val));;
    const cliente = createSelectorRx({ id: idCliente }, x => {
        var b = new rx.BehaviorSubject<{ Direcciones: any[] }>({ Direcciones: [1, 2, 3] } as any);
        const isObs = isObservable(b);
        return b;
    });


    const clienteResult = await cliente.call({ props: {}, state: {} }).pipe(rxOps.first(), rxOps.toArray()).toPromise();

    expect(clienteResult).toEqual([{ Direcciones: [1, 2, 3] }]);

    const direcciones = createSelectorRx({ cli: cliente }, x => {
        return (x.cli && x.cli.Direcciones) || []
    });

    const direccionesSelect = createSelectorRx({ direcciones }, x => {
        return x.direcciones.map<{}>(x => ({ id: x, view: x }));
    });

    const obs = direcciones.call({ state: {}, props: { value: null, loading: false, } });
    const ret = await obs.pipe(rxOps.first(), rxOps.toArray()).toPromise();

    expect(ret).toEqual([[1, 2, 3]]);
});

test("selector con observable de observables", async () => {
    interface Props {
        a: number;
    };
    const a = toSelector((x: Props) => x.a);
    let count = 0;
    const contarA = createSelectorRx({ a }, s => {
        count++;
        return rx.timer(0, 350).pipe(rxOps.takeWhile(x => x < s.a));
    });

    const contarATest = async () => {
        const contarAObs = contarA.call({ a: 4 });
        const contarAArray = await contarAObs.pipe(rxOps.toArray()).toPromise();
        expect(contarAArray).toEqual([0, 1, 2, 3]);
    };

    await contarATest();
    await contarATest();

    const contarB = toSelector(() => rx.timer(0, 150).pipe(rxOps.takeWhile(x => x <= 5)));

    //Diagrama de los tiempos:

    /*
    T * 50ms  | a | b
    00        | 0 | 0
    03        | 0 | 1
    06        | 0 | 2
    07        | 1 | 2
    09        | 1 | 3
    12        | 1 | 4
    14        | 2 | 4
    15        | 2 | 5
    21        | 3 | 5 

    */
    const conteoPor2 = createSelectorRx({ a: contarA, b: contarB }, s => ({
        a: s.a,
        b: s.b
    }));

    const conteoPor2Obs = conteoPor2.call({ a: 4 });
    const result = await conteoPor2Obs.pipe(rxOps.toArray()).toPromise();
    const expected = [
        {a: 0, b: 0},
        {a: 0, b: 1},
        {a: 0, b: 2},
        {a: 1, b: 2},
        {a: 1, b: 3},
        {a: 1, b: 4},
        {a: 2, b: 4},
        {a: 2, b: 5},
        {a: 3, b: 5},
    ]
    expect(result).toEqual(expected);
});

test("selecotr con observable y nulo", async () => {
    interface Props {
        value?: number | null;
        loading?: boolean;
    }
    const idClienteDireccion = toSelector((props: Props) => props.value);
    const clienteDireccion = createSelectorRx({ id: idClienteDireccion }, s => {
        var b = new rx.BehaviorSubject<{ Cliente: number, IdCliente: number }>({ Cliente: 10, IdCliente: 1 } as any);
        const ret = s.id != null ? b : null;
        const ret2 = toObservable(ret as any);
        return ret2;
    });

    createSelectorRx({ a: idClienteDireccion, b: clienteDireccion }, s => s.b)

    const clienteFromValue = createSelectorRx({ x: clienteDireccion }, s => s.x && s.x.Cliente);
});