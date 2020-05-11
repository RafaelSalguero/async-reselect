import { createSelector, toSelector } from "./selector"
import { createSelectorAsync } from "./async";
import { syncResolve } from "simple-pure-utils";
test("selector simple test", () => {
    interface Props {
        a: number;
        b: number;
    }

    const a = toSelector((x: Props) => x.a);
    const b = toSelector((x: Props) => x.b);

    let a1Calls = 0;
    const a1 = createSelector({
        a: a
    }, (curr) => {
        a1Calls++;
        return curr.a + 5;
    });

    let b1Calls = 0;
    const b1 = createSelector({
        b: b
    }, (curr) => {
        b1Calls++;
        return curr.b + 2;
    });

    let abCalls = 0;
    const ab = createSelector({
        a: a1,
        b: b1
    }, curr => {
        abCalls++;
        return curr.a * curr.b;
    });

    {
        const r1 = ab.call({
            a: 2,
            b: 3
        });

        expect(r1).toBe(35);
        expect(a1Calls).toBe(1);
        expect(b1Calls).toBe(1);
        expect(abCalls).toBe(1);
    }

    {
        //Llamar de nuevo, con los mismos props:
        ab.call({
            a: 2,
            b: 3
        });

        expect(a1Calls).toBe(1);
        expect(b1Calls).toBe(1);
        expect(abCalls).toBe(1);
    }

    {
        //Cambiar sólo un prop:
        const r2 = ab.call({
            a: 1,
            b: 3
        });

        expect(r2).toBe(30);
        expect(a1Calls).toBe(2);
        expect(b1Calls).toBe(1);
        expect(abCalls).toBe(2);
    }
});

test("selector multiple", async () => {
    interface Props {
        a: number;
        b: number;
    }
    const a = toSelector((x: { x: Props, state: number }) => x.x.a);
    const b = toSelector((x: { x: Props, state: number }) => x.x.b);
    const c = toSelector((x: { x: Props, state: number }) => x.x.b + x.state);

    const sum = createSelector({ a, b, c }, x => x.a + x.b + x.c);

    expect(sum.call({ x: { a: 1, b: 2 }, state: 5 })).toBe(10);
});

test("curr y prev values", async () => {
    interface Props {
        a: number;
        b: number;
    }

    const a = toSelector((x: Props) => x.a);
    const b = toSelector((x: Props) => x.b);

    let values: { curr: Props, prev: Props | undefined }[] = [];
    const ab = createSelector({ a, b }, (curr, prev) => {
        values.push({ curr, prev });;

        return curr.a * curr.b;
    });

    expect(values).toEqual([]);

    ab.call({ a: 2, b: 3 });
    expect(values).toEqual([
        {
            curr: { a: 2, b: 3 },
            prev: undefined
        }
    ]);

    ab.call({ a: 2, b: 3 });
    expect(values.length).toBe(1);

    ab.call({ a: 2, b: 4 });
    expect(values).toEqual([
        {
            curr: { a: 2, b: 3 },
            prev: undefined
        }, {
            curr: { a: 2, b: 4 },
            prev: { a: 2, b: 3 }
        }
    ]);

});

test("curr y prev values async", async () => {
    interface Props {
        a: number;
        b: number;
    }

    const a = toSelector((x: Props) => syncResolve(x.a));
    const b = toSelector((x: Props) => syncResolve (x.b));

    let values: { curr: Props, prev: Props | undefined }[] = [];
    const ab = createSelectorAsync({ a, b }, (curr, prev) => {
        values.push({ curr, prev });;
        return curr.a * curr.b;
    });

    expect(values).toEqual([]);

    ab.call({ a: 2, b: 3 });
    expect(values).toEqual([
        {
            curr: { a: 2, b: 3 },
            prev: undefined
        }
    ]);

    ab.call({ a: 2, b: 3 });
    expect(values.length).toBe(1);

    ab.call({ a: 2, b: 4 });
    expect(values).toEqual([
        {
            curr: { a: 2, b: 3 },
            prev: undefined
        }, {
            curr: { a: 2, b: 4 },
            prev: { a: 2, b: 3 }
        }
    ]);

});