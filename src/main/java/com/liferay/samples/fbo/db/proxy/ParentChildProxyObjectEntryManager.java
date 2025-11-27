package com.liferay.samples.fbo.db.proxy;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.rest.dto.v1_0.ObjectEntry;
import com.liferay.object.rest.manager.v1_0.BaseObjectEntryManager;
import com.liferay.object.rest.manager.v1_0.ObjectEntryManager;
import com.liferay.object.service.ObjectDefinitionLocalService;
import com.liferay.petra.string.StringPool;
import com.liferay.portal.kernel.exception.PortalException;
import com.liferay.portal.kernel.json.JSONFactory;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.portal.kernel.search.Sort;
import com.liferay.portal.kernel.security.auth.CompanyThreadLocal;
import com.liferay.portal.kernel.service.CompanyLocalService;
import com.liferay.portal.vulcan.aggregation.Aggregation;
import com.liferay.portal.vulcan.dto.converter.DTOConverter;
import com.liferay.portal.vulcan.dto.converter.DTOConverterContext;
import com.liferay.portal.vulcan.pagination.Page;
import com.liferay.portal.vulcan.pagination.Pagination;
import com.liferay.samples.fbo.db.proxy.constants.DBProxyConstants;
import com.liferay.samples.fbo.db.proxy.datasource.DataSourceProvider;
import com.liferay.samples.fbo.db.proxy.helpers.FilterSQLQueryHelper;
import com.liferay.samples.fbo.db.proxy.helpers.IdManagementHelper;
import com.liferay.samples.fbo.db.proxy.helpers.PicklistHelper;
import com.liferay.samples.fbo.db.proxy.helpers.SearchSQLQueryHelper;
import com.liferay.samples.fbo.db.proxy.helpers.SortSQLQueryHelper;
import com.liferay.samples.fbo.db.proxy.helpers.SqlFragment;
import com.liferay.samples.fbo.db.proxy.helpers.SqlStringUtil;
import com.liferay.samples.fbo.db.proxy.mapping.DBField;
import com.liferay.samples.fbo.db.proxy.mapping.DBRel;
import com.liferay.samples.fbo.db.proxy.mapping.DBTableConfig;
import com.liferay.samples.fbo.db.proxy.mapping.DBTableConfigReaderUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
		property = "object.entry.manager.storage.type=" + DBProxyConstants.DB_PROXY + "_PARENT_CHILD",
		service = ObjectEntryManager.class
)
public class ParentChildProxyObjectEntryManager extends BaseObjectEntryManager
	implements ObjectEntryManager {
	
    @Override
    public ObjectEntry addObjectEntry(
            DTOConverterContext dtoConverterContext,
            ObjectDefinition objectDefinition,
            ObjectEntry objectEntry,
            String scopeKey) throws Exception {

    	final String objERC = objectDefinition.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);
    	
        final String table = config.getTableName();
        final Map<String, DBField> fields = config.getFields();
        final Map<String, DBRel> rels = config.getRels();

        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();

        Map<String, Object> props = objectEntry.getProperties() != null
                ? new LinkedHashMap<>(objectEntry.getProperties())
                : new LinkedHashMap<>();

	    String prefix = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getPrefix();

        Long explicitId = IdManagementHelper.extractIdFromValue(objectEntry.getExternalReferenceCode(), prefix);
        
        if (explicitId != null) {
            String idCol = fields.get(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();
            cols.add(idCol); vals.add(explicitId);
        }

        for (Map.Entry<String, DBField> e : fields.entrySet()) {
            String fname = e.getKey();
            if (DBProxyConstants.EXTERNAL_REFERENCE_CODE.equals(fname)) continue;

            DBField fdef = e.getValue();
            Object raw = props.get(fname);
            if (raw == null) continue;

            Object jdbcVal = toJdbcValueForType(raw, fdef.getType());
            cols.add(fdef.getDbColumn());
            vals.add(jdbcVal);
        }
        
        String insertSql = "INSERT INTO " + table + " (" + String.join(", ", cols) + ") VALUES ("
                + SqlStringUtil.nPlaceholders(cols.size()) + ")";

        Object generatedId = explicitId;

        try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
             java.sql.PreparedStatement ps = con.prepareStatement(insertSql, java.sql.Statement.RETURN_GENERATED_KEYS)) {

            bindParams(ps, vals);

            ps.executeUpdate();

            if (generatedId == null) {
                try (java.sql.ResultSet gk = ps.getGeneratedKeys()) {
                    if (gk != null && gk.next()) {
                        generatedId = gk.getObject(1);
                    }
                }
            }

            if (generatedId == null) {
                throw new IllegalStateException("Unable to determine generated ID for insert into " + table);
            }
        }

        String erc = StringPool.BLANK.equals(prefix)
                ? String.valueOf(generatedId)
                : config.generateExternalReferenceCode(generatedId, prefix);

        long companyId = CompanyThreadLocal.getCompanyId();
        
        for (Map.Entry<String, DBRel> e : rels.entrySet()) {
        	
        	String relName = e.getKey();
        	DBRel relDef = e.getValue();
        	
        	Object raw = props.get(relName);
        	if (raw == null) continue;
        	        	
        	ObjectDefinition objectDef = _objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(relDef.getObjectDefERC(), companyId);
        	
        	ObjectEntry relObjectEntry = new ObjectEntry();
        	
			String relPrefix = relDef.getForeignKeyFieldPrefix();
			String relType = relDef.getForeignKeyFieldType();
			
        	if(raw instanceof ArrayList) {
        		
        		ArrayList<Map<String, Object>> relPropsArray = (ArrayList<Map<String, Object>>) raw;

        		relPropsArray.forEach((relProps) -> {

        			Map<String, Object> extractedRelProps = new HashMap<>();
        			
        			relProps.put(relDef.getForeignKeyField(), IdManagementHelper.parseIdParamFromValue(objectEntry.getExternalReferenceCode(), relPrefix, relType));
        			relProps.forEach((key, value) -> {
        				if("externalReferenceCode".equals(key)) {
        					relObjectEntry.setExternalReferenceCode((String) value);
        				} else {
        					extractedRelProps.put(key, value);
        				}
        			});
        			relObjectEntry.setProperties(extractedRelProps);
                	try {
						addObjectEntry(dtoConverterContext, objectDef, relObjectEntry, scopeKey);
					} catch (Exception e1) {
						_log.warn("Failed to add related object", e1);
					}
        		});

        	}
        	
        }

        return getObjectEntry(objectDefinition.getCompanyId(), dtoConverterContext, erc, objectDefinition, scopeKey);
    }

    @Override
    public void deleteObjectEntry(
            long companyId,
            DTOConverterContext dtoConverterContext,
            String externalReferenceCode,
            ObjectDefinition objectDefinition,
            String scopeKey) throws Exception {

    	final String objERC = objectDefinition.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);

        final String table = config.getTableName();
        final String idCol = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

	    String prefix = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getPrefix();

        Object idParam = IdManagementHelper.parseIdParamFromValue(externalReferenceCode, prefix, DBProxyConstants.TYPE_INTEGER);

        String sql = "DELETE FROM " + table + " WHERE " + idCol + " = ?";

        try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
             java.sql.PreparedStatement ps = con.prepareStatement(sql)) {

            int i = 1;
            ps.setObject(i++, idParam);

            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new java.util.NoSuchElementException("No entry found to delete for ERC=" + externalReferenceCode);
            }
        }
    }

    @Override
    public ObjectEntry updateObjectEntry(
            long companyId,
            DTOConverterContext dtoConverterContext,
            String externalReferenceCode,
            ObjectDefinition objectDefinition,
            ObjectEntry objectEntry,
            String scopeKey) throws Exception {

    	final String objERC = objectDefinition.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);
    	
        final String table  = config.getTableName();
        final Map<String, DBField> fields = config.getFields();
        final String idCol = fields.get(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

	    String prefix = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getPrefix();

        Object idParam = IdManagementHelper.parseIdParamFromValue(externalReferenceCode, prefix, DBProxyConstants.TYPE_INTEGER);

        List<String> sets = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        Map<String, Object> props = objectEntry.getProperties() != null
                ? new LinkedHashMap<>(objectEntry.getProperties())
                : new LinkedHashMap<>();

        for (Map.Entry<String, DBField> e : fields.entrySet()) {
            String fname = e.getKey();
            if (DBProxyConstants.EXTERNAL_REFERENCE_CODE.equals(fname)) continue;

            if (!props.containsKey(fname)) continue;

            DBField fdef = e.getValue();
            Object raw = props.get(fname);

            Object jdbcVal = toJdbcValueForType(raw, fdef.getType());
            sets.add(fdef.getDbColumn() + " = ?");
            params.add(jdbcVal);
        }

        if (sets.isEmpty()) {
            return getObjectEntry(companyId, dtoConverterContext, externalReferenceCode, objectDefinition, scopeKey);
        }

        String sql = "UPDATE " + table + " SET " + String.join(", ", sets)
                + " WHERE " + idCol + " = ?";

        boolean update = true;
        
        try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
             java.sql.PreparedStatement ps = con.prepareStatement(sql)) {

            int i = 1;

            for (Object p : params) {
                i = bindOne(ps, i, p);
            }

            ps.setObject(i++, idParam);

            int affected = ps.executeUpdate();
            if (affected == 0) {
            	update = false;
                //throw new java.util.NoSuchElementException("No entry found to update for ERC=" + externalReferenceCode);
            }
        }

        if(!update) {
           	addObjectEntry(dtoConverterContext, objectDefinition, objectEntry, scopeKey);
           	// Any thrown exception will be thrown further and prevent related records insertions / updates
        }
        
        final Map<String, DBRel> rels = config.getRels();
        
        for (Map.Entry<String, DBRel> relEntry : rels.entrySet()) {

            final String relName = relEntry.getKey();
            final DBRel relDef = relEntry.getValue();

            Object raw = props.get(relName);
            if (raw == null) {
                continue;
            }

            ObjectDefinition childObjectDef =
                _objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(
                    relDef.getObjectDefERC(), companyId);

            final String parentFkPrefix = relDef.getForeignKeyFieldPrefix();
            final String parentFkType   = relDef.getForeignKeyFieldType();
            final String fkFieldName    = relDef.getForeignKeyField();

            if (raw instanceof List) {
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> childPropsList = (List<Map<String, Object>>) raw;

                for (Map<String, Object> childProps : childPropsList) {

                    ObjectEntry childEntry = new ObjectEntry();

                    Map<String, Object> extractedChildProps = new HashMap<>();
                    String childERC = null;

                    Object parentDbId =
                        IdManagementHelper.parseIdParamFromValue(
                            externalReferenceCode, parentFkPrefix, parentFkType);
                    childProps.put(fkFieldName, parentDbId);

                    for (Map.Entry<String, Object> p : childProps.entrySet()) {
                        if ("externalReferenceCode".equals(p.getKey())) {
                            childERC = (String) p.getValue();
                        } else {
                            extractedChildProps.put(p.getKey(), p.getValue());
                        }
                    }

                    if (childERC != null) {
                        childEntry.setExternalReferenceCode(childERC);
                    }
                    childEntry.setProperties(extractedChildProps);
                    
                    if (childERC != null && existsObjectEntry(companyId, childObjectDef, childERC, dtoConverterContext, scopeKey)) {
                    	updateObjectEntry(companyId, dtoConverterContext, childERC, childObjectDef, childEntry, scopeKey);
                    	/*
                        if (_log.isDebugEnabled()) {
                            _log.debug("Skip existing related object " + childERC + " for relation " + relName);
                        }
                        */
                        continue;
                    }

                    try {
                        addObjectEntry(dtoConverterContext, childObjectDef, childEntry, scopeKey);
                    } catch (Exception ex) {
                        _log.warn("Failed to add related object for relation " + relName, ex);
                    }
                }
            }
        }        

        return getObjectEntry(companyId, dtoConverterContext, externalReferenceCode, objectDefinition, scopeKey);
    }

	
	@Override
	public Page<ObjectEntry> getObjectEntries(
		long companyId, ObjectDefinition objectDefinition, String scopeKey,
		Aggregation aggregation, DTOConverterContext dtoConverterContext,
		String filterString, Pagination pagination, String search,
		Sort[] sorts) throws Exception {

    	final String objERC = objectDefinition.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);
    	
		String table = config.getTableName();
		Map<String, String> fieldToColumn = new LinkedHashMap<>();
		for (Map.Entry<String, DBField> e : config.getFields().entrySet()) {
			fieldToColumn.put(e.getKey(), e.getValue().getDbColumn());
		}

		java.util.List<String> searchableCols = config.getSearchableColumns();

		String baseWhere = "";
		java.util.List<Object> baseParams = new java.util.ArrayList<>();
		baseParams.add(companyId);
		baseParams.add(scopeKey);

		try (java.sql.Connection con = _dsProvider.getDataSource().getConnection()) {
			String dbProduct = con.getMetaData().getDatabaseProductName();

			SqlFragment filterFrag = FilterSQLQueryHelper.build(
				    filterString,
				    fieldToColumn,
				    config.getFields(),
				    zoneIdFromContext(dtoConverterContext)
				);
			SqlFragment searchFrag = SearchSQLQueryHelper.build(search, searchableCols, dbProduct);
			String defaultOrderBy = IdManagementHelper.computeDefaultOrderBy(config);
			String orderBy = SortSQLQueryHelper.build(sorts, fieldToColumn, defaultOrderBy);

			List<String> whereClauses = new ArrayList<>();
			List<Object> allParams = new ArrayList<>();

			if (baseWhere != null && !baseWhere.trim().isEmpty()) {
			    whereClauses.add(baseWhere.trim());
			    allParams.add(companyId);
			    allParams.add(scopeKey);
			}

			if (!filterFrag.isEmpty()) {
			    whereClauses.add("(" + filterFrag.getSql() + ")");
			    allParams.addAll(filterFrag.getParams());
			}

			if (!searchFrag.isEmpty()) {
			    whereClauses.add("(" + searchFrag.getSql() + ")");
			    allParams.addAll(searchFrag.getParams());
			}

			String whereSql = whereClauses.isEmpty() ? "" : " WHERE " + String.join(" AND ", whereClauses);

			String selectSql = "SELECT * FROM " + table + whereSql + " " + orderBy;
			String countSql  = "SELECT COUNT(*) FROM " + table + whereSql;

			long total;
			try (java.sql.PreparedStatement cps = con.prepareStatement(countSql)) {
			    bindParams(cps, allParams); // <- pas baseParams + frags, mais la liste agrégée
				try (java.sql.ResultSet rs = cps.executeQuery()) {
					rs.next();
					total = rs.getLong(1);
				}
			}

			String pagedSql = applyPagination(dbProduct, selectSql,
			    pagination.getStartPosition(),
			    pagination.getEndPosition() - pagination.getStartPosition(),
			    defaultOrderBy
			);
			
			java.util.List<ObjectEntry> items = new java.util.ArrayList<>();
			try (java.sql.PreparedStatement ps = con.prepareStatement(pagedSql)) {
			    bindParams(ps, allParams);
				try (java.sql.ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						items.add(mapRowToObjectEntry(objectDefinition, rs, dtoConverterContext));
					}
				}
			}

			return Page.of(items, pagination, Math.toIntExact(total));
		}
	}

	@Override
	public ObjectEntry getObjectEntry(
	    long companyId, DTOConverterContext dtoConverterContext,
	    String externalReferenceCode, ObjectDefinition objectDefinition,
	    String scopeKey) throws Exception {

		String nestedFieldsParam = null;
		
		try {
			nestedFieldsParam = dtoConverterContext.getHttpServletRequest().getParameter("nestedFields");
		} catch(Exception e) {
			_log.debug("Could not read nestedFieldsParam", e);
		}
		
		Collection<String> nestedFields;
		
		if(nestedFieldsParam == null) {
			nestedFields = Collections.emptyList();
		} else {
			nestedFields = Stream.of(nestedFieldsParam.split(","))
	                .collect(Collectors.toCollection(ArrayList::new));
		}

		
    	final String objERC = objectDefinition.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);
		
	    String table = config.getTableName();
	    String idColumn = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

	    String sql = "SELECT * FROM " + table + " WHERE " + idColumn + " = ?";

	    try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
	         java.sql.PreparedStatement ps = con.prepareStatement(sql)) {

		    String prefix = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getPrefix();

		    Object idParam = IdManagementHelper.parseIdParamFromValue(externalReferenceCode, prefix, DBProxyConstants.TYPE_INTEGER);
	        ps.setObject(1, idParam);

	        try (java.sql.ResultSet rs = ps.executeQuery()) {
	            if (!rs.next()) {
	                throw new java.util.NoSuchElementException("No entry found for ERC: " + externalReferenceCode);
	            }
	            
	            ObjectEntry objectEntry = mapRowToObjectEntry(objectDefinition, rs, dtoConverterContext);
	            
	            config.getRels().forEach((key, rel) -> {
	            	if(nestedFields.contains(key)) {
	            		String relatedObjectDefinitionExternalReferenceCode = rel.getObjectDefERC(); 
	            		ObjectDefinition relatedObjectDefinition;
						try {
							relatedObjectDefinition = _objectDefinitionLocalService.getObjectDefinitionByExternalReferenceCode(relatedObjectDefinitionExternalReferenceCode, companyId);
							
							Object fkValue = IdManagementHelper.parseIdParamFromValue(
							        externalReferenceCode,
							        rel.getForeignKeyFieldPrefix(),
							        rel.getForeignKeyFieldType()
							);

							String filterString = rel.getForeignKeyField() + " eq " + toODataLiteral(fkValue, rel.getForeignKeyFieldType());

							try {
								Collection<ObjectEntry> relatedObjectEntries = getObjectEntries(companyId, relatedObjectDefinition, scopeKey,
										null, dtoConverterContext,
										filterString, Pagination.of(0, 200), StringPool.BLANK,
										null).getItems();
								
								mapRelatedObjectEntriesToProps(objectEntry, key, rel, relatedObjectEntries);
								
							} catch (Exception e) {
								_log.warn("Failed to add related object entries", e);
							}

						} catch (PortalException e) {
							_log.warn("Failed to get related object definition", e);
						}
	            	}
	            });
	            
	            return objectEntry;
	        }
	    }
	}
	
	private void mapRelatedObjectEntriesToProps(
	        ObjectEntry parentObjectEntry,
	        String relName,
	        DBRel relDef,
	        Collection<ObjectEntry> relatedObjectEntries) {

	    final String fkField = relDef.getForeignKeyField();

	    final java.util.Set<String> excludedFields =
	            new java.util.HashSet<>(java.util.Arrays.asList(fkField));

	    java.util.List<Map<String, Object>> mapped = new java.util.ArrayList<>();

	    for (ObjectEntry child : relatedObjectEntries) {
	        Map<String, Object> childOut = new LinkedHashMap<>();

	        childOut.put("externalReferenceCode", child.getExternalReferenceCode());

	        Map<String, Object> props = child.getProperties();
	        if (props != null) {
	            for (Map.Entry<String, Object> e : props.entrySet()) {
	                String name = e.getKey();
	                if (!excludedFields.contains(name)) {
	                    childOut.put(name, e.getValue());
	                }
	            }
	        }

	        mapped.add(childOut);
	    }

	    parentObjectEntry.getProperties().put(relName, mapped);
	}

	private int bindOne(PreparedStatement ps, int i, Object p) throws SQLException {
	    if (p == null) {
	        ps.setObject(i++, null);
	        return i;
	    }

	    if (p instanceof java.math.BigDecimal) {
	        ps.setBigDecimal(i++, (java.math.BigDecimal) p);
	    } else if (p instanceof Long) {
	        ps.setLong(i++, (Long) p);
	    } else if (p instanceof Integer) {
	        ps.setInt(i++, (Integer) p);
	    } else if (p instanceof Short) {
	        ps.setShort(i++, (Short) p);
	    } else if (p instanceof Byte) {
	        ps.setByte(i++, (Byte) p);
	    } else if (p instanceof Double) {
	        ps.setDouble(i++, (Double) p);
	    } else if (p instanceof Float) {
	        ps.setFloat(i++, (Float) p);
	    } else if (p instanceof Boolean) {
	        ps.setBoolean(i++, (Boolean) p);
	    } else if (p instanceof java.sql.Timestamp) {
	        ps.setTimestamp(i++, (java.sql.Timestamp) p);
	    } else if (p instanceof java.util.Date) {
	        ps.setTimestamp(i++, new java.sql.Timestamp(((java.util.Date) p).getTime()));
	    } else {
	        ps.setString(i++, String.valueOf(p));
	    }
	    return i;
	}

	private void bindParams(PreparedStatement ps, List<Object> params) throws SQLException {
	    int i = 1;
	    for (Object p : params) {
	        i = bindOne(ps, i, p);
	    }
	}

	private Object toJdbcValueForType(Object raw, String type) {
	    if (raw == null) return null;
	    String t = (type == null) ? "text" : type.toLowerCase(java.util.Locale.ROOT);

	    switch (t) {
	        case DBProxyConstants.TYPE_PICKLIST:
	            if (raw instanceof java.util.Map) {
	                Object key = ((java.util.Map<?, ?>) raw).get("key");
	                return (key == null) ? null : String.valueOf(key);
	            }
	            return String.valueOf(raw);

	        case DBProxyConstants.TYPE_DECIMAL:
	            if (raw instanceof java.math.BigDecimal) return raw;
	            if (raw instanceof Number) return new java.math.BigDecimal(raw.toString());
	            try { return new java.math.BigDecimal(String.valueOf(raw)); }
	            catch (NumberFormatException e) { return null; }

	        case DBProxyConstants.TYPE_INTEGER:
	            if (raw instanceof Long) return raw;
	            if (raw instanceof Integer) return raw;
	            
	        case DBProxyConstants.TYPE_DATE:
	        case DBProxyConstants.TYPE_DATETIME:
	            if (raw instanceof java.util.Date) {
	                return new java.sql.Timestamp(((java.util.Date) raw).getTime());
	            }
	            if (raw instanceof java.time.Instant) {
	                return java.sql.Timestamp.from((java.time.Instant) raw);
	            }
	            if (raw instanceof java.time.LocalDate) {
	                java.time.LocalDate d = (java.time.LocalDate) raw;
	                java.time.ZonedDateTime z = d.atStartOfDay(java.time.ZoneId.systemDefault());
	                return java.sql.Timestamp.from(z.toInstant());
	            }
	            if (raw instanceof java.time.LocalDateTime) {
	                java.time.LocalDateTime dt = (java.time.LocalDateTime) raw;
	                return java.sql.Timestamp.valueOf(dt);
	            }
	            String s = String.valueOf(raw).trim();
	            if (s.matches("\\d{4}-\\d{2}-\\d{2}$")) {
	                java.time.LocalDate d = java.time.LocalDate.parse(s);
	                java.time.ZonedDateTime z = d.atStartOfDay(java.time.ZoneId.systemDefault());
	                return java.sql.Timestamp.from(z.toInstant());
	            }
	            try {
	                if (s.endsWith("Z") || s.matches(".*[+-]\\d{2}:?\\d{2}$")) {
	                    return java.sql.Timestamp.from(java.time.OffsetDateTime.parse(s).toInstant());
	                } else {
	                    String norm = s.replace(' ', 'T');
	                    return java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(norm));
	                }
	            } catch (Exception ignore) {
	                try { return java.sql.Timestamp.from(java.time.Instant.parse(s)); }
	                catch (Exception e) { return null; }
	            }

	        case DBProxyConstants.TYPE_TEXT:
	        default:
	            return String.valueOf(raw);
	    }
	}
	
	private java.time.ZoneId zoneIdFromContext(DTOConverterContext ctx) {
	    Locale loc = (ctx != null && ctx.getLocale() != null) ? ctx.getLocale() : Locale.getDefault();
	    return java.util.Optional.ofNullable(loc)
	            .map(l -> l.toLanguageTag())
	            .map(tag -> java.time.ZoneId.systemDefault())
	            .orElse(java.time.ZoneId.systemDefault());
	}
	
	private ObjectEntry mapRowToObjectEntry(
	        ObjectDefinition def,
	        java.sql.ResultSet rs,
	        DTOConverterContext ctx) throws java.sql.SQLException, PortalException {

    	final String objERC = def.getExternalReferenceCode();

    	DBTableConfig config = DBTableConfigReaderUtil.getDBTableConfig(objERC);
		
	    ObjectEntry entry = new ObjectEntry();

	    DBField ercField = config.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE);
	    Object rawId = null;
	    try {
	        rawId = rs.getObject(ercField.getDbColumn());
	    } catch (java.sql.SQLException ignore) { }

	    if (rawId != null) {
	    	String prefix = ercField.getPrefix();
	        if (StringPool.BLANK.equals(prefix)) {
	            entry.setExternalReferenceCode(String.valueOf(rawId));
	        } else {
	            entry.setExternalReferenceCode(config.generateExternalReferenceCode(rawId, prefix));
	        }
	    }

	    Map<String, Object> props = new LinkedHashMap<>();
	    for (Map.Entry<String, DBField> e : config.getFields().entrySet()) {
	        String fieldName = e.getKey();
	        if (DBProxyConstants.EXTERNAL_REFERENCE_CODE.equals(fieldName)) continue;

	        DBField fieldDef = e.getValue();
	        String col = fieldDef.getDbColumn();

	        try {
	            Object val = rs.getObject(col);
	            if (val == null) continue;        

	            if (DBProxyConstants.TYPE_PICKLIST.equalsIgnoreCase(fieldDef.getType())) {
	                String key = String.valueOf(val);
	                String name = PicklistHelper.resolvePicklistName(def, fieldName, key, ctx);
	                Map<String, Object> pick = new LinkedHashMap<>();
	                pick.put("key", key);
	                if (name != null) pick.put("name", name);
	                props.put(fieldName, pick);
	            } else {
	            	String prefix = fieldDef.getPrefix();
	            	if(val instanceof String && !StringPool.BLANK.equals(prefix)) {
		                props.put(fieldName, prefix + (String) val);
		        	} else {
		                props.put(fieldName, val);
		        	}
	            }
	        } catch (java.sql.SQLException ignore) { }
	    }

	    entry.setProperties(props);
	    return entry;
	}
	
	private boolean existsObjectEntry(
	        long companyId,
	        ObjectDefinition objectDefinition,
	        String externalReferenceCode,
	        DTOConverterContext dtoConverterContext,
	        String scopeKey) {
	    try {
	        getObjectEntry(companyId, dtoConverterContext, externalReferenceCode, objectDefinition, scopeKey);
	        return true;
	    } catch (Exception e) {
	        return false;
	    }
	}
	
	private String applyPagination(String dbProductName, String sql, int offset, int limit, String defaultOrderBy) {
	    String product = (dbProductName == null ? "" : dbProductName).toLowerCase(java.util.Locale.ROOT);

	    if (limit < 0) limit = 0;
	    if (offset < 0) offset = 0;

	    if (product.contains("postgresql")) {
	        return sql + " LIMIT " + limit + " OFFSET " + offset;
	    }

	    if (product.contains("sql server")) {
	        String lower = sql.toLowerCase(java.util.Locale.ROOT);
	        if (!lower.contains("order by")) {
	            String order = (defaultOrderBy == null || defaultOrderBy.trim().isEmpty()) ? "1" : defaultOrderBy.trim();
	            sql = sql + " ORDER BY " + order;
	        }
	        return sql + " OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
	    }

	    return sql + " LIMIT " + limit + " OFFSET " + offset;
	}
	
	private String toODataLiteral(Object value, String type) {
	    if (value == null) return "null";

	    String t = (type == null) ? DBProxyConstants.TYPE_TEXT : type.toLowerCase(java.util.Locale.ROOT);

	    switch (t) {
	        case DBProxyConstants.TYPE_INTEGER:
	        case DBProxyConstants.TYPE_DECIMAL:
	            return String.valueOf(value);

	        case DBProxyConstants.TYPE_PICKLIST:
	        case DBProxyConstants.TYPE_TEXT:
	            String s = String.valueOf(value).replace("'", "''");
	            return "'" + s + "'";

	        case DBProxyConstants.TYPE_BOOLEAN:
	            return String.valueOf(value);

	        case DBProxyConstants.TYPE_DATE:
	        case DBProxyConstants.TYPE_DATETIME:
	            String iso;
	            if (value instanceof java.time.temporal.TemporalAccessor) {
	                if (value instanceof java.time.Instant) {
	                    iso = java.time.OffsetDateTime.ofInstant((java.time.Instant) value, java.time.ZoneOffset.UTC).toString();
	                } else if (value instanceof java.time.LocalDateTime) {
	                    iso = ((java.time.LocalDateTime) value).toString();
	                } else if (value instanceof java.time.LocalDate) {
	                    iso = ((java.time.LocalDate) value).toString();
	                } else {
	                    iso = value.toString();
	                }
	            } else if (value instanceof java.util.Date) {
	                iso = java.time.OffsetDateTime.ofInstant(((java.util.Date) value).toInstant(), java.time.ZoneOffset.UTC).toString();
	            } else {
	                iso = String.valueOf(value);
	            }
	            iso = iso.replace("'", "''");
	            return "'" + iso + "'";

	        default:
	            String def = String.valueOf(value).replace("'", "''");
	            return "'" + def + "'";
	    }
	}	

	@Override
	public String getStorageLabel(Locale locale) {
		return DBProxyConstants.DB_PROXY_LABEL + " [PARENT_CHILD]";
	}

	@Override
	public String getStorageType() {
		return DBProxyConstants.DB_PROXY+ "_PARENT_CHILD";
	}

	@Reference
	private CompanyLocalService _companyLocalService;

	@Reference
	private JSONFactory _jsonFactory;

	@Reference(
		target = "(component.name=com.liferay.object.rest.internal.dto.v1_0.converter.ObjectEntryDTOConverter)"
	)
	private DTOConverter<com.liferay.object.model.ObjectEntry, ObjectEntry>
		_objectEntryDTOConverter;

	@Reference
	private ObjectDefinitionLocalService _objectDefinitionLocalService;

    @Reference
    private DataSourceProvider _dsProvider;
	
    private static final Log _log = LogFactoryUtil.getLog(ParentChildProxyObjectEntryManager.class);
    
}
