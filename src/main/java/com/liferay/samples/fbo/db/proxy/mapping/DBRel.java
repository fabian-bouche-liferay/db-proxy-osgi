package com.liferay.samples.fbo.db.proxy.mapping;

public class DBRel {
    private final String dbTable;
    private final String objectDefERC;
    private final String foreignKeyField;
    private final String foreignKeyFieldPrefix;
    private final String foreignKeyFieldType;

    public DBRel(String dbTable, String objectDefERC, String foreignKeyField, String foreignKeyFieldPrefix, String foreignKeyFieldType) {
        this.dbTable = dbTable;
        this.objectDefERC = objectDefERC;
        this.foreignKeyField = foreignKeyField;
        this.foreignKeyFieldPrefix = foreignKeyFieldPrefix;
        this.foreignKeyFieldType = foreignKeyFieldType;
    }

    public String getDbTable() { return dbTable; }

    public String getObjectDefERC() { return objectDefERC; }
    
    public String getForeignKeyField() { return foreignKeyField; }

	public String getForeignKeyFieldPrefix() { return foreignKeyFieldPrefix; }

	public String getForeignKeyFieldType() { return foreignKeyFieldType; }

    @Override
    public String toString() {
        return "DBRel{" +
                "dbTable='" + dbTable + '\'' +
                "objectDefERC='" + objectDefERC + '\'' +
                "foreignKeyField='" + foreignKeyField + '\'' +
                '}';
    }

}