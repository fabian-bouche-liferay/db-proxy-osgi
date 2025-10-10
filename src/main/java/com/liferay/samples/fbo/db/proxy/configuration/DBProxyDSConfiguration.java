package com.liferay.samples.fbo.db.proxy.configuration;

import com.liferay.portal.configuration.metatype.annotations.ExtendedObjectClassDefinition;

import aQute.bnd.annotation.metatype.Meta;

@ExtendedObjectClassDefinition(
	category = "object",
	scope = ExtendedObjectClassDefinition.Scope.COMPANY		
)
@Meta.OCD(
	id = "com.liferay.samples.fbo.db.proxy.configuration.DBProxyDSConfiguration"
)
public interface DBProxyDSConfiguration {

    @Meta.AD(
    		required = false,
    		name = "JNDI Name",
    	    description = "JNDI Datasource Name",
    		deflt = "java:comp/env/jdbc/ProxyDS"    		
    )
    String jndiName();

    @Meta.AD(
    		required = false,
    		name = "Schema",
    	    description = "Schema name to prefix table names",
    		deflt = ""
    )
    String schema();
    
}
