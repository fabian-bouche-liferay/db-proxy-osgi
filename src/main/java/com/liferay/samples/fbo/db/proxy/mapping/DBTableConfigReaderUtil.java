package com.liferay.samples.fbo.db.proxy.mapping;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.model.ObjectEntry;
import com.liferay.object.model.ObjectRelationship;
import com.liferay.object.service.ObjectDefinitionLocalServiceUtil;
import com.liferay.object.service.ObjectEntryLocalServiceUtil;
import com.liferay.object.service.ObjectRelationshipLocalServiceUtil;
import com.liferay.petra.string.StringPool;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.security.auth.CompanyThreadLocal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBTableConfigReaderUtil {

	public static final String PROXY_TABLES = "PROXY_TABLES";

	public static final String PROXY_TABLE_FIELDS = "PROXY_TABLE_FIELDS";
	public static final String PROXY_TABLE_FIELDS_REL = "proxyTableFieldRel";
	
	public static final String PROXY_TABLE_RELATIONSHIPS = "PROXY_TABLE_RELATIONSHIPS";
	public static final String PROXY_TABLE_RELATIONSHIPS_REL = "proxyTableRelationsRel";
	
	public static DBTableConfig getDBTableConfig(String objectDefinitionExternalReferenceCode) throws PortalException {
		
		long companyId = CompanyThreadLocal.getCompanyId();
		
		ObjectDefinition proxyTablesObjectDefinition = ObjectDefinitionLocalServiceUtil.getObjectDefinitionByExternalReferenceCode(PROXY_TABLES, companyId);
		ObjectEntry tableConfigObjectEntry = ObjectEntryLocalServiceUtil.getObjectEntry(objectDefinitionExternalReferenceCode, proxyTablesObjectDefinition.getObjectDefinitionId());
		
		String tableName = (String) tableConfigObjectEntry.getValues().get("tableName");
		
		ObjectRelationship proxyTableFieldRel = ObjectRelationshipLocalServiceUtil.getObjectRelationship(proxyTablesObjectDefinition.getObjectDefinitionId(), PROXY_TABLE_FIELDS_REL);
		ObjectRelationship proxyTableRelationsRel = ObjectRelationshipLocalServiceUtil.getObjectRelationship(proxyTablesObjectDefinition.getObjectDefinitionId(), PROXY_TABLE_RELATIONSHIPS_REL);
		
		Map<String, DBField> fields = new HashMap<>();
		List<ObjectEntry> fieldObjectEntries = ObjectEntryLocalServiceUtil.getOneToManyObjectEntries(0l, proxyTableFieldRel.getObjectRelationshipId(), tableConfigObjectEntry.getObjectEntryId(), true, StringPool.BLANK, -1, -1);		
		
		fieldObjectEntries.forEach(objectEntry -> {
			String objectFieldName = (String) objectEntry.getValues().get("objectFieldName");
			String dbColumn = (String) objectEntry.getValues().get("tableFieldName");
			String type = (String) objectEntry.getValues().get("tableFieldType");
			boolean searchable = (boolean) objectEntry.getValues().get("searchable");
			String prefix = (String) objectEntry.getValues().get("valuePrefix");
			DBField dbField = new DBField(dbColumn, type, searchable, prefix);
			fields.put(objectFieldName, dbField);
		});
		
		Map<String, DBRel> rels = new HashMap<>();
		List<ObjectEntry> relObjectEntries = ObjectEntryLocalServiceUtil.getOneToManyObjectEntries(0l, proxyTableRelationsRel.getObjectRelationshipId(), tableConfigObjectEntry.getObjectEntryId(), true, StringPool.BLANK, -1, -1);

		relObjectEntries.forEach(objectEntry -> {
			String relName = (String) objectEntry.getValues().get("objectRelationshipName");
			String dbTable = (String) objectEntry.getValues().get("relatedTableName");
			String relatedObjectDefERC = (String) objectEntry.getValues().get("relatedObjectDefinitionERC");
			String foreignKeyField = (String) objectEntry.getValues().get("foreignKeyRelatedTableFieldName");
			String foreignKeyFieldPrefix = (String) objectEntry.getValues().get("valuePrefix");
			String foreignKeyFieldType = (String) objectEntry.getValues().get("foreignKeyRelatedTableFieldType");
			DBRel dbRel = new DBRel(dbTable, relatedObjectDefERC, foreignKeyField, foreignKeyFieldPrefix, foreignKeyFieldType);
			rels.put(relName, dbRel);
		});
		
		DBTableConfig dbTableConfig = new DBTableConfig(tableName, fields, rels);
		
		return dbTableConfig;		
	}
	
}
