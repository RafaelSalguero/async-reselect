import { selectorCacheRequest, SelectorCache, SelectorCacheResponse } from "./cache";
import { mapObject } from "simple-pure-utils";


/**
 * El selector tiene una función que toma una entrada en forma de un objeto y genera una salida. Esta función debe de ser pura considerando
 * como argumentos las propiedades de primer nivel de la entrada, es decir, para las mismas propiedades debe de devolver la misma salida.
 * Para esto, el selector tiene un cache que recuerda la ultima llamada a la función, este cache se puede limpiar llamando a @see clear
 */
export interface Selector<TIn extends {}, TOut> {
    /**Obtiene el valor del selector */
    call(input: TIn): TOut;
    /**Obtiene el valor del selector sin usar el cache */
    raw(input: TIn): TOut;
    /**Limpia el cache del selector */
    clear(): void;
}



export type SelectorOutType<T> = T extends Selector<any, infer R> ? R : never;
export type SelectorInType<T> = T extends  Selector<infer R, any> ? R : never;
export type SelectorMap<TIn> = {
    [K in string]: Selector<TIn, any>
}

export type SelectorMapOuts<T extends SelectorMap<any>> = {
    [K in keyof T]: SelectorOutType<T[K]>
};


export type SelectorMapIn<T extends SelectorMap<any>> = SelectorInType<T[keyof T]>;

/**Una función que mapea un objeto resultante de multiples selectores a una salida */
export interface SelectorMapFunc<TIn, TOut> {
    (curr: TIn, prev: TIn | undefined): TOut;
}

/**
 * Ejecuta un selector y devuelve el resultado del selector y el nuevo cache. Esta es una función pura
 * @param cache El caché actual
 * @param dependsOn Los selectores de los que depende este selector
 * @param input La entrada actual
 * @param map La función que dado el resultado de dependsOn obtiene la salida del selector
 */
export function runSelector<TOut, TDeps extends {}>(
    cache: SelectorCache<TDeps, TOut> | undefined,
    selectorResults: TDeps,
    map: SelectorMapFunc<TDeps, TOut>,
    options: SelectorOptions
    ): SelectorCacheResponse<TDeps, TOut> {
    //Checar el cache:
    const req = selectorCacheRequest(cache, {
        func: map,
        args: selectorResults
    }, options);

    return req;
}

/**
 * Ejecuta un selector y devuelve el resultado del selector y el nuevo cache. Esta es una función pura
 * @param cache El caché actual
 * @param dependsOn Los selectores de los que depende este selector
 * @param input La entrada actual
 * @param map La función que dado el resultado de dependsOn obtiene la salida del selector
 */
export function runSelectorDeps<TOut, TDeps extends SelectorMap<any>>(
    cache: SelectorCache<SelectorMapOuts<TDeps>, TOut> | undefined,
    dependsOn: TDeps,
    input: SelectorMapIn<TDeps>,
    map: SelectorMapFunc<SelectorMapOuts<TDeps>, TOut>,
    options: SelectorOptions
    ): SelectorCacheResponse<SelectorMapOuts<TDeps>, TOut> {
    //Llama a todos los selectores con el input:
    const selectorResults = mapObject(dependsOn, val => val.call(input)) as SelectorMapOuts<TDeps>;

    return runSelector(cache, selectorResults, map, options);
}

/**Ejecuta la función del selector directamente sin ningún tipo de cache */
export function runSelectorRaw<TOut, TDeps extends SelectorMap<any>>( dependsOn: TDeps, map: SelectorMapFunc<SelectorMapOuts<TDeps>, TOut>, input: SelectorMapOuts<TDeps>) {
    const selectorResults = mapObject(dependsOn, val => val.call(input)) as SelectorMapOuts<TDeps>;
    return map(selectorResults, undefined);
}


/**Convierte una función pura a un selector */
export function toSelector<TIn, TOut>(func: (input: TIn) => TOut): Selector<TIn, TOut> {
    return {
        call: func,
        raw: func,
        clear: () => { }
    };
}

export interface SelectorOptions {
   /**Si la comparación de los argumentos debe de hacerse recursivamente */ 
    comparer?: (a: any, b: any) => boolean;
}


/**
 * Crea una selector, el cual es tiene una funcíón que depende de otros selectores.
 * Si todas sus dependencias devuelven la misma salida el selector devuelve exactamente la misma salida sin evaluar la función map
 * @param dependsOn Las dependencias del selector, es un objeto de selectores
 * @param map Evalua el selector en caso de que alguna de las dependencias haya cambiado de resultado
 */
export function createSelector<TOut, TDeps extends SelectorMap<any>>(dependsOn: TDeps, map: SelectorMapFunc<SelectorMapOuts<TDeps>, TOut>, options? : SelectorOptions ): Selector<SelectorMapIn<TDeps>, TOut> {
    type SelOuts = SelectorMapOuts<TDeps>;
    let cache: SelectorCache<SelOuts, TOut> | undefined = undefined;

    const ret = (input: SelectorMapIn<TDeps>) => {
        const response = runSelectorDeps(cache, dependsOn, input, map, options || {});
        cache = response.cache;
        return response.result;
    };

    return {
        call: ret,
        raw: (input) => runSelectorRaw(dependsOn, map, input),
        clear: () => cache = undefined
    };
}