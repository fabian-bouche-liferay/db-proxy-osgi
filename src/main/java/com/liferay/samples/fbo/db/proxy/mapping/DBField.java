package com.liferay.samples.fbo.db.proxy.mapping;

public class DBField {
    private final String dbColumn;
    private final String type;
    private final boolean searchable;
    private final boolean prefix;

    public DBField(String dbColumn, String type) {
        this(dbColumn, type, false, false);
    }

    public DBField(String dbColumn, String type, boolean searchable, boolean prefix) {
        this.dbColumn = dbColumn;
        this.type = type;
        this.searchable = searchable;
        this.prefix = prefix;
    }

    public String getDbColumn() { return dbColumn; }
    public String getType() { return type; }
    public boolean isSearchable() { return searchable; }
    public boolean hasPrefix() { return prefix; }

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