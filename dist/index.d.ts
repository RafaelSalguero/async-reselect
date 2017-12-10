export { pipe } from "./pipe";
export { nullsafe } from "./nullsafe";
export { addDate, DateUnits, truncateDate } from "./dates";
import * as rx from "rxjs";
export { search } from "./search";
export { removeDiacritics } from "./diacritics";
/**Devuelve true si todos los elementos de un arreglo encajan con el predicado */
export declare function all<T>(arr: T[], pred: (x: T) => boolean): boolean;
/**Devuelve true si por lo menos un elemento del arreglo encaja con el predicado, o si existe por lo menos un elemento en caso
 * de que el predicado este indefinido
 */
export declare function any<T>(arr: T[], pred?: (x: T) => boolean): boolean;
/**Devuelve true si el valor existe en el arreglo */
export declare function contains<T>(arr: T[], value: T, comparer?: (a: T, b: T) => boolean): boolean;
/**Devuelve true si todos los valores en @see values existen en el arreglo @see arr . Si @see values esta vacío devuelve true */
export declare function containsAll<T>(arr: T[], values: T[], comparer?: (a: T, b: T) => boolean): boolean;
/**Devuelve true si existe algun valor en @see values que exista en @see arr . Si @see values esta vacío devuelve false */
export declare function containsAny<T>(arr: T[], values: T[], comparer?: (a: T, b: T) => boolean): boolean;
/**
 * Alias para el operador ===
 * @param a
 * @param b
 */
export declare function referenceEquals<T>(a: T, b: T): boolean;
/**Compara dos arreglos valor por valor */
export declare function sequenceEquals<T>(a: T[], b: T[], comparer?: (a: T, b: T) => boolean): boolean;
/**Devuelve true si 2 arreglos contienen los mismos valores, sin considerar el orden o la cantidad de veces que el mismo valor esta repetido en el arreglo
 * @param comparer Función que se usa para comparar los elementos, si no se especifica, se usa el referenceEquals
 */
export declare function setEquals<T>(a: T[], b: T[], comparer?: (a: T, b: T) => boolean): boolean;
/**Compara dos objetos propiedad por propiedad */
export declare function shallowEquals<T>(a: T, b: T, comparer?: (a: T[keyof T], b: T[keyof T]) => boolean): boolean;
/**Resultado de un shallow diff */
export declare type ShallowDiffResult<T> = {
    [K in keyof T]?: true;
};
export declare type ObjSet<T> = {
    [K in keyof T]?: true;
};
/**
 * Compara 2 objetos propiedad por propiedad, devuelve un objeto con las propiedades que son diferentes asignadas a true
 * @param a Objeto a
 * @param b Objecto b
 * @param comparer Comparador de las propiedades. Se usa por default referenceEquals
 */
export declare function shallowDiff<T>(a: T, b: T, comparer?: (a: T[keyof T], b: T[keyof T]) => boolean): ObjSet<T>;
/**Convierte un ArrayLike o Iterable en un arreglo. Si el valor ya es un arreglo devuelve el valor */
export declare function toArray<T>(arr: ArrayLike<T> | Iterable<T>): T[];
/**Devuelve true si un objeeto se puede convertir a un arreglo utilizando la función toArray */
export declare function canBeArray(arr: any): arr is ArrayLike<any> | Iterable<any>;
/**Devuelve true si x es un array o un array like */
export declare function isArrayLike(x: any): x is ArrayLike<any>;
export declare function deepEquals<T>(a: T, b: T): boolean;
/**Convierte un arreglo a un objeto */
export declare function toMap<T, TValue>(arr: T[], key: (value: T) => string, value: (value: T) => TValue): {
    [index: string]: TValue;
};
/**Aplana una colección de colecciones */
export declare function flatten<T>(arr: T[][]): T[];
/**Devuelve el primer elemento de un arreglo o indefinido si no se encontro ninguno, opcionalmente
 * filtrado por un predicado
 */
export declare function first<T>(arr: T[], pred?: (item: T) => boolean): T | undefined;
/**
 * Devuelve el unico elemento de un arreglo que cumpla con la condición, si no se encontró ninguo o mas de uno devuelve null
 */
export declare function single<T>(arr: T[], pred?: (item: T) => boolean): T | undefined;
/**Devuelve el ultimo elemento de un arreglo */
export declare function last<T>(arr: T[]): T | undefined;
export declare type Grouping<TKey, TItem> = {
    key: TKey;
    items: TItem[];
};
/**Agrupa un arreglo por una llave. Se preserva el orden original de los elementos del arreglo, segun los elementos agrupadores que aparezcan primero, tambien
 * el orden adentro del grupo es preservado
 * @param comparer Comparador, por default es un shallowEquals
 */
export declare function groupBy<T, TKey>(arr: T[], groupBy: (item: T) => TKey, comparer?: (a: TKey, b: TKey) => boolean): Grouping<TKey, T>[];
export interface ObjMap<T> {
    [key: string]: T;
}
/**Enumera todas las propiedades de un objeto en un arreglo
 * @param obj Objeto que se va a enumerar. Se devulve un arreglo de {value: T, key: string}
 */
export declare function enumObject<T>(obj: T): ({
    key: keyof T;
    value: T[keyof T];
})[];
/**
 * Enumera todas las propiedades de un objeto en un arreglo
 * @param obj Objeto que se va a enumerar
 * @param selector Función que obtiene cada elemento del arreglo
 */
export declare function enumObject<T, TR>(obj: T, selector: (key: keyof T, value: T[keyof T]) => TR): TR[];
/**
 * Convierte un arreglo en un objeto
 * @param array Arreglo donde se toma la propiedad "key" de cada elemento como key del objeto
 */
export declare function arrayToMap<TKey extends string, TValue>(array: {
    key: TKey;
    value: TValue;
}[]): ObjMap<TValue>;
/**
 * Convierte un arreglo a un objeto
 * @param array Arreglo de valores
 * @param keySelector Función que obtiene la cadena que se tomada como el "key" de cada elemento
 */
export declare function arrayToMap<T, TValue>(array: T[], keySelector: (item: T) => string, valueSelector: (item: T) => TValue): ObjMap<TValue>;
/**
 * Aplica una función a cada propiedad de un objeto, conservando los keys
 * @param obj Objeto a mapear
 * @param map Función que toma el valor y el "key" y devuelve el nuevo valor
 */
export declare function mapObject<T, TOut>(obj: T, map: <K extends keyof T>(value: T[K], key: K) => TOut): {
    [K in keyof T]: TOut;
};
/**
 * Filtra las propiedades de un objeto
 * @param obj Objeto que se va a filtrar
 * @param pred Predicado que va a determinar que propiedades si se van a conservar
 */
export declare function filterObject<T extends {
    [key: string]: any;
}>(obj: T, pred: (value: T[keyof T], key: keyof T) => boolean): T;
/**
 * Quita un conjunto de propiedades de un objeto
 * @param obj El objeto original
 * @param keys Las propiedades que se desean quitar
 */
export declare function omit<T>(obj: T, keys: (keyof T)[]): T;
/**Quita las propiedades que esten indefinidas en un objeto */
export declare function omitUndefined<T>(obj: T): Partial<T>;
/**Intercambia 2 elementos de un arreglo, si los indices dados estan afuera del arreglo, lanza una excepción */
export declare function swapItems<T>(array: T[], a: number, b: number): T[];
/**Mueve un elemento del arreglo de un indice a otro, note que no es igual a swapItems ya que al mover un elemento se conserva el orden de todos los de más elemento, esto no ocurre con el swap que
 * simplemente intercambia de posición 2 elementos. Si los indices estan fuera de rango lanza uan excepción
*/
export declare function moveItem<T>(array: T[], sourceIndex: number, destIndex: number): T[];
/**Mueve un elemento hacia array o hacia abajo, si el elemento no se puede mover ya que esta en el borde del arreglo devuelve el arreglo tal cual */
export declare function upDownItem<T>(array: T[], index: number, direction: "up" | "down"): T[];
export declare type Promisify<T> = {
    [K in keyof T]: PromiseLike<T[K]>;
};
/**Aplica una función Promise.all a un objeto,  */
export declare function promiseAllObj<T>(obj: Promisify<T>): Promise<T>;
/**Convierte una promesa de un objeto a un objeto de promesas
 * @param include Nombres de las propiedades que se desean incluir en el objeto resultante
 */
export declare function awaitObj<T, TKeys extends keyof T>(obj: PromiseLike<T>, include: {
    [K in TKeys]: true;
}): Promisify<Pick<T, TKeys>>;
/**Devuelve todos los elementos de un arreglo que no estan repetidos, respetando el orden original en el que aparecen primero.
 * @param comparer Comparador que determina si 2 elementos son iguales. Se usa el operador ===
*/
export declare function unique<T>(arr: T[], comparer?: (a: T, b: T) => boolean): T[];
/**Devuelve todos los elementos de todos los arreglos que no esten repetidos */
export declare function union<T>(...arr: T[][]): T[];
/**Pega todos los elementos de los arreglos */
export declare function concat<T>(...arr: T[][]): T[];
/**Filtra el arreglo sólo si condition == true, si es false devuelve el arreglo tal cual */
export declare function filterIf<T>(arr: T[], predicate: (item: T) => boolean, condition: boolean): T[];
/**Dado un arreglo de keys, para cada key mapea a el elemento que le corresponde.
 * Si existen varios elementos con la misma clave, cuando se encuentre esa clave se devolverá el primer elemento en el arreglo values con esa clave
 * @param keys Claves que se van a mapear
 * @param values Valores en los que se va a buscar para cada clave, el valor que tiene esa clave
 * @param keySelector Obtener la clave de un elemento
 * @param keyComparer Comparador que se usará para determinar si dos claves son iguales. Por default se usa el shallowEquals
 */
export declare function mapKeys<T, TKey>(keys: TKey[], values: T[], keySelector: (item: T) => TKey, keyComparer?: (a: TKey, b: TKey) => boolean): T[];
/**Devuelve todos los elementos en "a" que se encuentren también en "b". Conserva el orden original de "a"
 * @param comparer Comparedor de igualdad. Por default se usa el referenceEquals
 */
export declare function intersect<T>(a: T[], b: T[], comparer?: (a: T, b: T) => boolean): T[];
/**Devuelve todos los elementos en "items" tal que su key se encuentre una o mas veces en "keys". Conserva el orden original de "items".
 * @param keySelector Obtiene la clave de un elemento
 * @param comparer Comparedor de igualdad. Por default se usa el shallowEquals
 */
export declare function intersectKeys<T, TKey>(items: T[], keys: TKey[], keySelector: (item: T) => TKey, comparer?: (a: TKey, b: TKey) => boolean): T[];
/**Devuelve un rango de numeros */
export declare function range(start: number, count: number, step?: number): number[];
/**
 * Devuelve un nuevo arreglo con todo el arreglo original mas el elemento al final
 */
export declare function push<T>(arr: T[], item: T): T[];
/**
 * Remplaza todos los valores del arreglo que cumplan con cierta condicion
 */
export declare function replace<T>(arr: T[], condition: (item: T, index: number) => boolean, newValue: T): T[];
/**Elimina un elemento del arreglo */
export declare function remove<T>(arr: T[], item: T): T[];
/**
 * Combina varias funciones comparadores que pueden ser usadas para alimentar a la función sort. Se le da prioridad a los primeros comparadores,
 * si un comparador devuelve 0, entonces se evalue el segundo
 * @param comparers
 */
export declare function combineComparers<T>(...comparers: ((a: T, b: T) => number)[]): (a: T, b: T) => number;
/**Comparador de ordenamiento por default */
export declare function defaultComparer<T>(a: T, b: T): number;
export declare type ComparerFunction<T> = (a: T, b: T) => number;
/**Ordena un arreglo de forma estable, a diferencia de con array.sort el arreglo original no es modificado
 * @param comparers Comparadores de ordenamiento, se le da precedencia al primero. Si no se especifica ninguno se usará el comparador por default
 */
export declare function sort<T>(arr: T[], ...comparers: (ComparerFunction<T>)[]): T[];
/**
 * Ordena un arreglo de forma estable segun ciertas claves seleccionadas usando el comparador por default
 */
export declare function orderBy<T>(arr: T[], ...keySelectors: ((x: T) => any)[]): T[];
/**Ordena un arreglo de forma estable y descendiente segun ciertas claves seleccionadas usando el comparador por default */
export declare function orderByDesc<T>(arr: T[], ...keySelectors: ((x: T) => any)[]): T[];
/**Convierte un observable de T, de Promise<T> o de Observable<T> a un observable de <T>, efectivamente aplanando un observable anidado en uno desanidado */
export declare function rxFlatten<T>(observable: rx.Observable<T | PromiseLike<T> | rx.Observable<T>>): rx.Observable<T>;
/**Convierte un valor o una promesa a un observable, si el valor ya es un observable lo devuelve tal cual */
export declare function toObservable<T>(value: T | PromiseLike<T> | rx.Observable<T>): rx.Observable<T>;
/**Toma los primeros N elementos del arreglo */
export declare function take<T>(arr: T[], count: number): T[];
/**Obtiene le primer elemento mapeado de un arreglo o undefined */
export declare function firstMap<T, R>(arr: T[], predicate: (x: T) => boolean, map: (x: T) => R): R | undefined;
/**Devuelve true si existiran duplicados en caso de editar un elemento de un arreglo
 * @param arr Arreglo
 * @param oldValueRef Valor anterior del arreglo
 * @param newValue Nuevo valor del arreglo
 */
export declare function duplicatesOnEdit<T, TKey>(arr: T[], oldValue: T, newValue: T, keySelector: (x: T) => TKey): boolean;
/**
 * Devuelve true si existirán duplicados en caso de agregar un elemento a un arreglo que es equivalente a saber
 * si ese elemento esta contenido en el arreglo
 * @param arr
 * @param newValue
 * @param comparer  Se usa el shallow equals por default
 */
export declare function duplicatesOnAdd<T, TKey>(arr: T[], newValue: T, keySelector: (x: T) => TKey): boolean;
/**Devuelve true si x tiene el metodo then, lo que indica que es una promesa */
export declare function isPromise(x: any): x is PromiseLike<any>;
/**Devuelve true si x es un observable */
export declare function isObservable(x: any): x is rx.Observable<any>;
/**Devuelve true si x es un array */
export declare function isArray(x: any): x is any[];
/**Mapea el valor actual y el anterior de un observable */
export declare function mapPreviousRx<T>(obs: rx.Observable<T>, startWith: T): rx.Observable<{
    prev: T;
    curr: T;
}>;
/**Mapea cada elemento de un arreglo tomando en cuenta el elemento anterior */
export declare function mapPrevious<T, TR>(items: T[], map: (prev: T, curr: T) => TR, initial: T): TR[];
/**Calcula un agregado corrido para cada elemento de un arreglo */
export declare function runningTotal<TIn, TState, TOut>(items: TIn[], seed: TState, reduceState: (state: TState, item: TIn) => TState, map: (state: TState, item: TIn) => TOut): TOut[];
/**Mapea y aplana una colección. Es equivalente a  flatten(items.map(map)) */
export declare function mapMany<T, TR>(items: T[], map: (x: T) => TR[]): TR[];
/**
 * Formatea un número
 * @param number El numero
 * @param integer Cantidad de zeros a la izquierda en la parte entera
 * @param decimals Cantidad de zeros a la derecha en la parte desimal
 * @param thousep Usar separador de miles. Por default es false
 * @param prefix Prefijo del numero, ejemplo $ o %. Por default es ""
 */
export declare function formatNumber(number: number | null | undefined | string, integer?: number, decimals?: number, thousep?: boolean, prefix?: string): string;
/**Formatea una fecha
 * @param fullDateTime true o false para indicar si mostrar las horas o no. Por default es undefined e implicar que se mostraran las horas si el valor tiene componente de horas, si no, se mostrará sólo la fecha
 */
export declare function formatDate(x: Date | null | undefined | string, fullDateTime?: boolean): string;
/**Formatea una fecha de tal manera que sea compatible con el excel */
export declare function formatDateExcel(x: Date): string;
/**Devuelve una copia de una función. Respeta las propiedades agregadas a la función */
export declare function cloneFunction<T extends (...args: any[]) => any>(func: T): T;
/**Aplica un Function.bind respetando las propiedades agregadas a la función. Tambien se almacena la funcion original de tal manera que se puede devolver al estado original */
export declare function bindFunction<T extends (...args: any[]) => any>(func: T, thisArg?: any, ...argArray: any[]): T;
/**Deshace un bind aplicado con bindFunction. Un bind aplicado con Function.bind directamente no se puede deshacer. Si al argumento no se le fue aplicado un bind devuelve undefined */
export declare function unbindFunction<T extends (...args: any[]) => any>(func: T): T | undefined;