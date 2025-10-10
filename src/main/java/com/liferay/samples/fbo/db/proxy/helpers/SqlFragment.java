package com.liferay.samples.fbo.db.proxy.helpers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SqlFragment {
    private final String sql;
    private final List<Object> params;

    public SqlFragment(String sql, List<Object> params) {
        this.sql = sql == null ? "" : sql;
        this.params = params == null ? List.of() : List.copyOf(params);
    }

    public String getSql() { return sql; }

    public List<Object> getParams() { return params; }

    public static SqlFragment empty() {
        return new SqlFragment("", List.of());
    }

    public SqlFragment and(SqlFragment other) {
        if (this.isEmpty()) return other;
        if (other == null || other.isEmpty()) return this;
        return new SqlFragment("(" + this.sql + ") AND (" + other.sql + ")",
                concat(this.params, other.params));
    }

    public SqlFragment or(SqlFragment other) {
        if (this.isEmpty()) return other;
        if (other == null || other.isEmpty()) return this;
        return new SqlFragment("(" + this.sql + ") OR (" + other.sql + ")",
                concat(this.params, other.params));
    }

    public boolean isEmpty() { return sql.isBlank(); }

    private static <T> List<T> concat(List<T> a, List<T> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        List<T> out = new ArrayList<>(a.size() + b.size());
        out.addAll(a); out.addAll(b);
        return Collections.unmodifiableList(out);
    }
}