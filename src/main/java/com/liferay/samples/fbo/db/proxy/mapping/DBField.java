package com.liferay.samples.fbo.db.proxy.mapping;

import com.liferay.petra.string.StringPool;

public class DBField {
    private final String dbColumn;
    private final String type;
    private final boolean searchable;
    private final String prefix;

    public DBField(String dbColumn, String type) {
        this(dbColumn, type, false, StringPool.BLANK);
    }

    public DBField(String dbColumn, String type, boolean searchable, String prefix) {
        this.dbColumn = dbColumn;
        this.type = type;
        this.searchable = searchable;
        this.prefix = prefix;
    }

    public String getDbColumn() { return dbColumn; }
    public String getType() { return type; }
    public boolean isSearchable() { return searchable; }
    public String getPrefix() { return prefix; }

    @Override
    public String toString() {
        return "DBField{" +
                "dbColumn='" + dbColumn + '\'' +
                ", type='" + type + '\'' +
                ", searchable=" + searchable +
                ", prefix=" + prefix +
                '}';
    }
}