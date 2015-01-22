/*
 *  Copyright (c) 2013 Andrea Esposito <and1989@gmail.com>
 */

package it.unipi.common;

/**
 * @author Andrea Esposito <and1989@gmail.com>
 */
public class Random {

    public static int nextInt(int idPeer, int iteration) {
        return (int) hash(idPeer + iteration);
    }

    private static long hash
            (long x) {
        x = 3935559000370003845L * x + 2691343689449507681L;
        x = x ^ (x >>> 21);
        x = x ^ (x << 37);
        x = x ^ (x >>> 4);
        x = 4768777513237032717L * x;
        x = x ^ (x << 20);
        x = x ^ (x >>> 41);
        x = x ^ (x << 5);
        return x;
    }
}
