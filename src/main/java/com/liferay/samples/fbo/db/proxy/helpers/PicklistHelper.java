package com.liferay.samples.fbo.db.proxy.helpers;

import com.liferay.list.type.model.ListTypeEntry;
import com.liferay.list.type.service.ListTypeEntryLocalServiceUtil;
import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectField;
import com.liferay.object.service.ObjectFieldLocalServiceUtil;
import com.liferay.portal.vulcan.dto.converter.DTOConverterContext;

import java.util.List;

public class PicklistHelper {

	public static String resolvePicklistName(
	        ObjectDefinition objectDefinition,
	        String fieldName,
	        String key,
	        DTOConverterContext ctx) {

	    if (key == null) return null;

	    long ltdId = 0;
	    try {
	    	
	    	List<ObjectField> objectFields = ObjectFieldLocalServiceUtil.getObjectFields(objectDefinition.getObjectDefinitionId());
	        for (ObjectField of : objectFields) {
	            if (fieldName.equals(of.getName())) {
	                ltdId = of.getListTypeDefinitionId();
	                break;
	            }
	        }
	    } catch (Throwable ignored) {}

	    if (ltdId <= 0) return null;

	    ListTypeEntry entry = ListTypeEntryLocalServiceUtil.fetchListTypeEntry(ltdId, key);
	    if (entry == null) return null;

	    java.util.Locale locale = (ctx != null && ctx.getLocale() != null)
	        ? ctx.getLocale()
	        : java.util.Locale.getDefault();

	    try {
	        return entry.getName(locale);
	    } catch (Throwable ignored) {
	        return entry.getName(java.util.Locale.getDefault());
	    }
	}
	
}
