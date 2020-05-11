import { toSelector } from "../selector";
import { createSelectorAsync, createSelectorRx } from "./async";
import { delay, isObservable } from "simple-pure-utils";
import * as rx from "rxjs";
import * as rxOps from "rxjs/operators";

test("selector al tener error no debe de memoizar ASYNC", async () => {
    const a = toSelector(() => 10);
    let count = 0;
    const sum = createSelectorAsync({a}, async x => {
        count++;
        await delay(100);
        if (count >= 3) return x.a;
        else
            throw new Error("Error de prueba");
    });

    let errCount = 0;
    try { await sum.call({}); } catch (error) { errCount++; }
    try { await sum.call({}); } catch (error) { errCount++; }

    //Se demuestra que una promesa fallida no se guarda en el cache,
    //si así fuera, el cache devolvería la promesa fallida y esta linea
    //lanzaría excepción
    expect(await sum.call({})).toBe(10);

    expect(errCount).toBe(2);
    expect(count).toBe(3);

    await sum.call({});
    await sum.call({});

    expect(errCount).toBe(2);
    expect(count).toBe(3);
});


test("selector con observable que lanza error", async () => {
    interface Props {
        a: number;
    };
    const a = toSelector((x: Props) => x.a);
    let count = 0;
    const contarA = createSelectorRx({a}, a => {
        count++;
        if (count == 2) {
            return rx.Observable.throw("Error de prueba");
        }
        return rx.timer(0, 100).pipe(rxOps.takeWhile(x => x < a.a));
    });
    const conteoPor2 = createSelectorRx({contarA}, a => a.contarA * 2);

    const conteoPor2Obs = conteoPor2.call({ a: 2 });
    let obs2 = await conteoPor2Obs.pipe(rxOps.toArray()).toPromise();
    expect(obs2).toEqual([0, 2]);

    obs2 = await conteoPor2.call({ a: 2 }).pipe(rxOps.toArray()).toPromise();
    expect(obs2).toEqual([0, 2]);
    expect(count).toBe(1);

    let errCount = 0;
    try {
        const obs = conteoPor2.call({ a: 3 });
        let obs3 = await obs.pipe(rxOps.toArray()).toPromise();
    } catch (error) {
        errCount++;
    }

    expect(errCount).toBe(1);
    expect(count).toBe(2);

    //Una segunda llamada con los mismos argumentos SI ocasiona llamada, ya que la llamada anterior lanzó una excepción
    const obs = conteoPor2.call({ a: 3 });
    let obs3 = await obs.pipe(rxOps.toArray()).toPromise();
    expect(obs3).toEqual([0, 2, 4]);
    expect(errCount).toBe(1);
    expect(count).toBe(3);
});
