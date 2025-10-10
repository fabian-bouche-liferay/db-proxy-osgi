# DB Proxy PoC

Enable the Feature Flag Proxy Object (LPS-135430)

Add the JDBC driver of the datasource to tomcat/lib

Add the Datasource in tomcat/conf/Catalina/localhost/ROOT.xml

Once the module is deployed, create an object using the DB Proxy
storage type. 

The table name and fields mapping are currently hardcoded
inside of `com.liferay.samples.fbo.db.proxy.DBProxyObjectEntryManager`

You have to map the name of the field in the object definition to the 
name of the column.

Replace them with your values for the test, like for example:

```
    private static final String DB_TABLE = "energy_distribution";

    private static final Map<String, DBField> FIELD_MAP;

    static {
        Map<String, DBField> map = new LinkedHashMap<>();

        map.put("externalReferenceCode",
            new DBField("id", "text", false, true));

        map.put("measurementDate",
            new DBField("measurement_date", "date"));

        map.put("energySource",
            new DBField("energy_source", "picklist"));

        map.put("distributionStatus",
            new DBField("status", "picklist"));

        map.put("energyKWH",
            new DBField("energy_kwh", "decimal"));

        map.put("region",
            new DBField("region", "text", true, false));

        FIELD_MAP = map;
    }

    private static final DBTableConfig CONFIG = new DBTableConfig(DB_TABLE, FIELD_MAP);	
```

