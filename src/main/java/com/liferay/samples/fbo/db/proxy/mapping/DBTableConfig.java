package com.liferay.samples.fbo.db.proxy.mapping;

import java.util.*;

public class DBTableConfig {

    private final String tableName;
    private final Map<String, DBField> fields;

    public DBTableConfig(String tableName, Map<String, DBField> fields) {
        this.tableName = tableName;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, DBField> getFields() {
        return fields;
    }

    public DBField getField(String name) {
        return fields.get(name);
    }

    public List<String> getSearchableColumns() {
        List<String> list = new ArrayList<>();
        for (DBField f : fields.values()) {
            if (f.isSearchable()) list.add(f.getDbColumn());
        }
        return list;
    }

    public String generateExternalReferenceCode(Object id) {
        DBField prefixField = fields.values().stream()
                .filter(DBField::hasPrefix)
                .findFirst()
                .orElse(null);
        if (prefixField == null || id == null) {
            return String.valueOf(id);
        }
        return tableName + "_" + id;
    }

    @Override
    public String toString() {
        return "DBTableConfig{" +
                "tableName='" + tableName + '\'' +
                ", fields=" + fields +
                '}';
    }
}
