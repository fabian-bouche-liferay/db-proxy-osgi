package com.liferay.samples.fbo.db.proxy;

import com.liferay.object.model.ObjectDefinition;
import com.liferay.object.rest.dto.v1_0.ObjectEntry;
import com.liferay.object.rest.manager.v1_0.BaseObjectEntryManager;
import com.liferay.object.rest.manager.v1_0.ObjectEntryManager;
import com.liferay.portal.kernel.json.JSONFactory;
import com.liferay.portal.kernel.search.Sort;
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
import com.liferay.samples.fbo.db.proxy.mapping.DBTableConfig;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(
		property = "object.entry.manager.storage.type=" + DBProxyConstants.DB_PROXY,
		service = ObjectEntryManager.class
)
public class DBProxyObjectEntryManager extends BaseObjectEntryManager
	implements ObjectEntryManager {

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
	
    @Override
    public ObjectEntry addObjectEntry(
            DTOConverterContext dtoConverterContext,
            ObjectDefinition objectDefinition,
            ObjectEntry objectEntry,
            String scopeKey) throws Exception {

        final String table = CONFIG.getTableName();
        final Map<String, DBField> fields = CONFIG.getFields();

        List<String> cols = new ArrayList<>();
        List<Object> vals = new ArrayList<>();

        Map<String, Object> props = objectEntry.getProperties() != null
                ? new LinkedHashMap<>(objectEntry.getProperties())
                : new LinkedHashMap<>();

        Long explicitId = IdManagementHelper.extractIdFromERC(objectEntry.getExternalReferenceCode(), CONFIG);
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

        String erc = (fields.get(DBProxyConstants.EXTERNAL_REFERENCE_CODE).hasPrefix())
                ? CONFIG.generateExternalReferenceCode(generatedId)
                : String.valueOf(generatedId);

        return getObjectEntry(objectDefinition.getCompanyId(), dtoConverterContext, erc, objectDefinition, scopeKey);
    }

    @Override
    public void deleteObjectEntry(
            long companyId,
            DTOConverterContext dtoConverterContext,
            String externalReferenceCode,
            ObjectDefinition objectDefinition,
            String scopeKey) throws Exception {

        final String table = CONFIG.getTableName();
        final String idCol = CONFIG.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

        Object idParam = IdManagementHelper.parseIdParamFromERC(externalReferenceCode, CONFIG);

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

        final String table  = CONFIG.getTableName();
        final Map<String, DBField> fields = CONFIG.getFields();
        final String idCol = fields.get(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

        Object idParam = IdManagementHelper.parseIdParamFromERC(externalReferenceCode, CONFIG);

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

        try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
             java.sql.PreparedStatement ps = con.prepareStatement(sql)) {

            int i = 1;

            for (Object p : params) {
                i = bindOne(ps, i, p);
            }

            ps.setObject(i++, idParam);

            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new java.util.NoSuchElementException("No entry found to update for ERC=" + externalReferenceCode);
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

		String table = CONFIG.getTableName();
		Map<String, String> fieldToColumn = new LinkedHashMap<>();
		for (Map.Entry<String, DBField> e : CONFIG.getFields().entrySet()) {
			fieldToColumn.put(e.getKey(), e.getValue().getDbColumn());
		}

		java.util.List<String> searchableCols = CONFIG.getSearchableColumns();

		String baseWhere = "";
		java.util.List<Object> baseParams = new java.util.ArrayList<>();
		baseParams.add(companyId);
		baseParams.add(scopeKey);

		try (java.sql.Connection con = _dsProvider.getDataSource().getConnection()) {
			String dbProduct = con.getMetaData().getDatabaseProductName();

			SqlFragment filterFrag = FilterSQLQueryHelper.build(
				    filterString,
				    fieldToColumn,
				    CONFIG.getFields(),
				    zoneIdFromContext(dtoConverterContext)
				);
			SqlFragment searchFrag = SearchSQLQueryHelper.build(search, searchableCols, dbProduct);
			String defaultOrderBy = IdManagementHelper.computeDefaultOrderBy(CONFIG);
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

	    String table = CONFIG.getTableName();
	    String idColumn = CONFIG.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE).getDbColumn();

	    String sql = "SELECT * FROM " + table + " WHERE " + idColumn + " = ?";

	    try (java.sql.Connection con = _dsProvider.getDataSource().getConnection();
	         java.sql.PreparedStatement ps = con.prepareStatement(sql)) {

	        Object idParam = IdManagementHelper.parseIdParamFromERC(externalReferenceCode, CONFIG);
	        ps.setObject(1, idParam);

	        try (java.sql.ResultSet rs = ps.executeQuery()) {
	            if (!rs.next()) {
	                throw new java.util.NoSuchElementException("No entry found for ERC: " + externalReferenceCode);
	            }
	            return mapRowToObjectEntry(objectDefinition, rs, dtoConverterContext);
	        }
	    }
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
	        DTOConverterContext ctx) throws java.sql.SQLException {

	    ObjectEntry entry = new ObjectEntry();

	    DBField ercField = CONFIG.getField(DBProxyConstants.EXTERNAL_REFERENCE_CODE);
	    Object rawId = null;
	    try {
	        rawId = rs.getObject(ercField.getDbColumn());
	    } catch (java.sql.SQLException ignore) { }

	    if (rawId != null) {
	        if (ercField.hasPrefix()) {
	            entry.setExternalReferenceCode(CONFIG.generateExternalReferenceCode(rawId));
	        } else {
	            entry.setExternalReferenceCode(String.valueOf(rawId));
	        }
	    }

	    Map<String, Object> props = new LinkedHashMap<>();
	    for (Map.Entry<String, DBField> e : CONFIG.getFields().entrySet()) {
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
	                props.put(fieldName, val);
	            }
	        } catch (java.sql.SQLException ignore) { }
	    }

	    entry.setProperties(props);
	    return entry;
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

	@Override
	public String getStorageLabel(Locale locale) {
		return DBProxyConstants.DB_PROXY_LABEL;
	}

	@Override
	public String getStorageType() {
		return DBProxyConstants.DB_PROXY;
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
    private DataSourceProvider _dsProvider;
	
}
