package com.liferay.samples.fbo.db.proxy.mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DBTableConfig {

    private final String tableName;
    private final Map<String, DBField> fields;
    private final Map<String, DBRel> rels;

    public DBTableConfig(String tableName, Map<String, DBField> fields) {
        this.tableName = tableName;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.rels = Collections.emptyMap();
    }

    public DBTableConfig(String tableName, Map<String, DBField> fields, Map<String, DBRel> rels) {
        this.tableName = tableName;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
        this.rels = Collections.unmodifiableMap(new LinkedHashMap<>(rels));
    }
    
    public String getTableName() {
        return tableName;
    }

    public Map<String, DBField> getFields() {
        return fields;
    }

    public Map<String, DBRel> getRels() {
        return rels;
    }
    
    public DBField getField(String name) {
        return fields.get(name);
    }

    public DBRel getRel(String name) {
        return rels.get(name);
    }

    public List<String> getSearchableColumns() {
        List<String> list = new ArrayList<>();
        for (DBField f : fields.values()) {
            if (f.isSearchable()) list.add(f.getDbColumn());
        }
        return list;
    }

    public String generateExternalReferenceCode(Object id, String prefix) {
        	return prefix + id;
    }

    @Override
    public String toString() {
        return "DBTableConfig{" +
                "tableName='" + tableName + '\'' +
                ", fields=" + fields +
                ", rel=" + rels +
                '}';
    }
}
