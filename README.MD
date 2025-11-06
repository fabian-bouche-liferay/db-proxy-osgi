# DB Proxy PoC

Enable the Feature Flag Proxy Object (LPS-135430)

Import the Proxy Table configuration Object Folder

Add the JDBC driver of the datasource to tomcat/lib

Add the Datasource in tomcat/conf/Catalina/localhost/ROOT.xml

Once the module is deployed, create an object using the DB Proxy
storage type. 

In addition to that, create the Proxy Table configuration.
