package com.liferay.samples.fbo.db.proxy.helpers;

import com.liferay.portal.kernel.search.Sort;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Construit un ORDER BY sécurisé (whitelist des colonnes).
 * Aucune valeur utilisateur ici.
 */
public final class SortSQLQueryHelper {

    private SortSQLQueryHelper() {}

    public static String build(Sort[] sorts,
                               Map<String, String> fieldToColumn,
                               String defaultOrder) {

        List<String> pieces = new ArrayList<>();

        if (sorts != null) {
            for (Sort s : sorts) {
                if (s == null) continue;
                String field = s.getFieldName();
                if (field == null) continue;

                String col = fieldToColumn.get(field);
                if (col == null) {
                    continue;
                }

                pieces.add(col + (s.isReverse() ? " DESC" : " ASC"));
            }
        }

        if (pieces.isEmpty()) {
            return orderByOrEmpty(defaultOrder);
        }

        StringBuilder sb = new StringBuilder("ORDER BY ");
        for (int i = 0; i < pieces.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(pieces.get(i));
        }
        return sb.toString();
    }
    
    private static String orderByOrEmpty(String defaultOrder) {
        if (defaultOrder == null) return "";
        String d = defaultOrder.trim();
        return d.isEmpty() ? "" : "ORDER BY " + d;
    }
}