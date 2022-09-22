package com.snoworca.IdxDB;

public enum OP {
    /** 같은 값(기본)*/
    eq,
	/** value 보다 더 큼 */
     gt,
	/**  value 보다 작음 */
    lt,
    /** value 보다 크거나 같음 */
    gte,
     /** value 보다 작거나 같은 */
    lte,
     /** value 와 일치하지 않음 */
    ne,
     /** value 가 포함되어 있음 */
    in,
     /** value 가 포함되어 있지 않다. */
     nin,
    and,
    or,
    not,
    nor;


    public static OP fromString(String value) {
        if("gt".equalsIgnoreCase(value)) return gt;
        else if("gte".equalsIgnoreCase(value)) return gte;
        else if("lt".equalsIgnoreCase(value)) return lt;
        else if("lte".equalsIgnoreCase(value)) return lte;
        else if("not".equalsIgnoreCase(value)) return not;
        else if("in".equalsIgnoreCase(value)) return in;
        else if("eq".equalsIgnoreCase(value)) return eq;

        return null;
    }

}
