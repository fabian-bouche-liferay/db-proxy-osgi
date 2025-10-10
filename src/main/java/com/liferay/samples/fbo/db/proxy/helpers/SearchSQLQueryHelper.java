package com.liferay.samples.fbo.db.proxy.helpers;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Recherche plein-texte multi-colonnes.
 * - Postgres: ILIKE
 * - Autres (ex. SQL Server): LOWER(col) LIKE LOWER(?)
 */
public final class SearchSQLQueryHelper {

    private SearchSQLQueryHelper() {}

    public static SqlFragment build(String search, List<String> searchableColumns, String dbProductName) {
        if (search == null || search.isBlank() || searchableColumns == null || searchableColumns.isEmpty()) {
            return SqlFragment.empty();
        }

        String token = "%" + search + "%";
        List<Object> params = new ArrayList<>();

        String product = dbProductName == null ? "" : dbProductName.toLowerCase(Locale.ROOT);
        final String clause;

        if (product.contains("postgresql")) {
            clause = searchableColumns.stream()
                    .map(col -> col + " ILIKE ?")
                    .collect(Collectors.joining(" OR "));
            for (int i = 0; i < searchableColumns.size(); i++) params.add(token);
        } else {
            clause = searchableColumns.stream()
                    .map(col -> "LOWER(" + col + ") LIKE LOWER(?)")
                    .collect(Collectors.joining(" OR "));
            for (int i = 0; i < searchableColumns.size(); i++) params.add(token);
        }

        return new SqlFragment(clause, params);
    }
}