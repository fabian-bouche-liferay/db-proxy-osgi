package com.liferay.samples.fbo.db.proxy.helpers;

import com.liferay.samples.fbo.db.proxy.constants.DBProxyConstants;
import com.liferay.samples.fbo.db.proxy.mapping.DBField;
import com.liferay.samples.fbo.db.proxy.mapping.DBTableConfig;

public class IdManagementHelper {

	public static Long extractIdFromERC(String erc, DBTableConfig config) {
	    if (erc == null || erc.trim().isEmpty()) return null;
	    if (config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).hasPrefix()) {
	        String prefix = config.getTableName() + "_";
	        if (erc.startsWith(prefix)) {
	            String idStr = erc.substring(prefix.length());
	            try { return Long.valueOf(idStr); } catch (NumberFormatException ignore) { return null; }
	        }
	        return null;
	    } else {
	        try { return Long.valueOf(erc); } catch (NumberFormatException ignored) { return null; }
	    }
	}

	public static Object parseIdParamFromERC(String erc, DBTableConfig config) {
	    if (erc == null) return null;
	    if (config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).hasPrefix()) {
	        String prefix = config.getTableName() + "_";
	        if (erc.startsWith(prefix)) {
	            String idStr = erc.substring(prefix.length());
	            try { return Long.valueOf(idStr); } catch (NumberFormatException e) { return idStr; }
	        }
	        // pas de préfixe → on tente long sinon string brute
	        try { return Long.valueOf(erc); } catch (NumberFormatException e) { return erc; }
	    } else {
	        try { return Long.valueOf(erc); } catch (NumberFormatException e) { return erc; }
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
