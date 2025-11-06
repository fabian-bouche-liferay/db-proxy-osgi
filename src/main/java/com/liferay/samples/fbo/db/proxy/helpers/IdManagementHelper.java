package com.liferay.samples.fbo.db.proxy.helpers;

import com.liferay.petra.string.StringPool;
import com.liferay.samples.fbo.db.proxy.constants.DBProxyConstants;
import com.liferay.samples.fbo.db.proxy.mapping.DBField;
import com.liferay.samples.fbo.db.proxy.mapping.DBTableConfig;

public class IdManagementHelper {

	public static Long extractIdFromValue(String value, String prefix) {
	    if (value == null || value.trim().isEmpty()) return null;
	    if (StringPool.BLANK.equals(prefix)) {
	        try { return Long.valueOf(value); } catch (NumberFormatException ignored) { return null; }
	    } else {
	        if (value.startsWith(prefix)) {
	            String idStr = value.substring(prefix.length());
	            try { return Long.valueOf(idStr); } catch (NumberFormatException ignore) { return null; }
	        }
	        return null;
	    }
	}

	public static Object parseIdParamFromValue(String value, String prefix, String type) {
	    if (value == null) return null;
	    if (StringPool.BLANK.equals(prefix)) {
	        try { return Long.valueOf(value); } catch (NumberFormatException e) { return value; }
	    } else {
	        if (value.startsWith(prefix)) {
	            String idStr = value.substring(prefix.length());
	            try {
	            	if(DBProxyConstants.TYPE_INTEGER.equals(type)) {
		            	return Long.valueOf(idStr);
	            	} else {
	            		return idStr;
	            	}
	            } catch (NumberFormatException e) { return idStr; }
	        }
	        try { return Long.valueOf(value); } catch (NumberFormatException e) { return value; }
	    }
	}
	
	public static String computeDefaultOrderBy(DBTableConfig config) {
	    DBField erc = config.getField("externalReferenceCode");
	    if (erc != null && erc.getDbColumn() != null && !erc.getDbColumn().isBlank()) {
	        return erc.getDbColumn() + " ASC";
	    }
	    for (DBField f : config.getFields().values()) {
	        if (f.getDbColumn() != null && !f.getDbColumn().isBlank()) {
	            return f.getDbColumn() + " ASC";
	        }
	    }
	    return "1 ASC";
	}
}
