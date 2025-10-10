package com.liferay.samples.fbo.db.proxy.helpers;

import java.util.Collections;

public final class SqlStringUtil {
    private SqlStringUtil() {}

    public static String nPlaceholders(int n) {
        if (n <= 0) throw new IllegalArgumentException("n must be > 0");
        return String.join(",", Collections.nCopies(n, "?"));
    }
}