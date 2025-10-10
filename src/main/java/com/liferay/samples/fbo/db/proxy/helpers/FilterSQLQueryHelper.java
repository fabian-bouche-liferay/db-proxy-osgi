package com.liferay.samples.fbo.db.proxy.helpers;

import com.liferay.samples.fbo.db.proxy.constants.DBProxyConstants;
import com.liferay.samples.fbo.db.proxy.mapping.DBField;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FilterSQLQueryHelper {

    private FilterSQLQueryHelper() {}

    private static final Pattern TOKEN = Pattern.compile(
    	    "(?<bool>\\band\\b|\\bor\\b)"
    	    + "|(?<field>[a-zA-Z_][a-zA-Z0-9_]*)\\s+"
    	    + "(?<op>not\\s+in|in|eq|ne|gt|ge|lt|le|like|contains|startswith|endswith)\\s+"
    	    + "(?<value>\\([^\\)]*\\)|'[^']*'|\\d{4}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?(?:Z|[+-]\\d{2}:?\\d{2})?)?|\\d+)",
    	    Pattern.CASE_INSENSITIVE
	);

    private static final Pattern NOT_IN_PATTERN = Pattern.compile(
    	    "\\bnot\\s*\\(\\s*([a-zA-Z_][a-zA-Z0-9_]*)\\s+in\\s*(\\([^\\)]*\\))\\s*\\)",
    	    Pattern.CASE_INSENSITIVE
	);

	private static String preNormalize(String s) {
	    if (s == null) return null;

	    try {
	        s = s.replace('+', ' ');
	        s = java.net.URLDecoder.decode(s, java.nio.charset.StandardCharsets.UTF_8.name());
	    } catch (Exception ignore) {}

	    String out = s;
	    Matcher m = NOT_IN_PATTERN.matcher(out);
	    StringBuffer sb = new StringBuffer();
	    boolean found = false;
	    while (m.find()) {
	        found = true;
	        String field = m.group(1);
	        String list  = m.group(2);
	        m.appendReplacement(sb, field + " not in " + list);
	    }
	    if (found) {
	        m.appendTail(sb);
	        out = sb.toString();
	    }
	    return out.trim();
	}

    public static SqlFragment build(String filterString,
                                    Map<String, String> fieldToColumn,
                                    Map<String, DBField> fieldMeta,
                                    ZoneId zoneId) {
        if (filterString == null || filterString.trim().isEmpty()) return SqlFragment.empty();
        
        filterString = preNormalize(filterString);
        
        if (zoneId == null) zoneId = ZoneId.systemDefault();

        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        Matcher m = TOKEN.matcher(filterString);
        boolean expectCondition = true;

        while (m.find()) {
            String bool = group(m, "bool");
            if (bool != null) {
                if (sql.length() > 0) {
                    sql.append(" ").append(bool.toUpperCase(Locale.ROOT)).append(" ");
                }
                expectCondition = true;
                continue;
            }

            String field = group(m, "field");
            String op    = group(m, "op");
            String raw   = group(m, "value");
            if (field == null || op == null || raw == null) continue;

            String col = fieldToColumn.get(field);
            if (col == null) continue;

            String type = Optional.ofNullable(fieldMeta.get(field))
                                  .map(DBField::getType)
                                  .map(String::toLowerCase)
                                  .orElse("text");

            if (!expectCondition && sql.length() > 0) {
                sql.append(" AND ");
            }

            String opNorm = op.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ");

            if (isDateType(type)) {
                appendDateComparison(sql, params, col, opNorm, raw, zoneId);
            } else {
                switch (opNorm) {
                    case "eq":
                    case "ne":
                    case "gt":
                    case "ge":
                    case "lt":
                    case "le":
                    case "like":
                        appendSimpleComparison(sql, params, col, opNorm, raw);
                        break;
                    case "contains":
                    case "startswith":
                    case "endswith":
                        appendLikeVariant(sql, params, col, opNorm, raw);
                        break;
                    case "in":
                    case "not in":
                        appendInList(sql, params, col, opNorm, raw, type, zoneId);
                        break;
                    default:
                }
            }

            expectCondition = false;
        }

        return new SqlFragment(sql.toString(), params);
    }

    private static boolean isDateType(String type) {
        return DBProxyConstants.TYPE_DATE.equalsIgnoreCase(type) || DBProxyConstants.TYPE_DATETIME.equalsIgnoreCase(type);
    }

    private static void appendDateComparison(StringBuilder sql, List<Object> params, String col,
            String op, String rawValue, ZoneId zoneId) {

    	if ("in".equals(op) || "not in".equals(op) || "contains".equals(op)
    			|| "startswith".equals(op) || "endswith".equals(op) || "like".equals(op)) {
			sql.append("1=1");
			return;
		}
		
		ParsedTemporal pt = parseTemporal(rawValue, zoneId);
		if (pt == null) {
			sql.append("1=0");
			return;
		}
		
		if (pt.isDateOnly) {
			java.sql.Timestamp start = java.sql.Timestamp.from(pt.startInstant);
			java.sql.Timestamp endEx = java.sql.Timestamp.from(pt.endExclusiveInstant);
			
			switch (op) {
			
				case "eq":
				sql.append("(").append(col).append(" >= ? AND ").append(col).append(" < ?)");
				params.add(start); params.add(endEx);
				return;
				
				case "ne":
				sql.append("(").append(col).append(" < ? OR ").append(col).append(" >= ?)");
				params.add(start); params.add(endEx);
				return;
				
				case "ge":
				sql.append(col).append(" >= ?");
				params.add(start);
				return;
				
				case "gt":
				sql.append(col).append(" >= ?");
				params.add(endEx);
				return;
				
				case "le":
				sql.append(col).append(" < ?");
				params.add(endEx);
				return;
				
				case "lt":
				sql.append(col).append(" < ?");
				params.add(start);
				return;
				
				default:
				sql.append("1=0");
				return;
			}
		}
		
		String sym;
		switch (op) {

			case "eq":
				sym = "=";
				break;
				
			case "ne": 
				sym = "<>"; 
				break;

			case "gt": 
				sym = ">"; 
				break;

			case "ge": 
				sym = ">="; 
				break;

			case "lt": 
				sym = "<"; 
				break;
				
			case "le": 
				sym = "<="; 
				break;
				
			default:   
				sym = "="; 
				break;
		}
		
		sql.append(col).append(" ").append(sym).append(" ?");
		params.add(java.sql.Timestamp.from(pt.instant));
	}
    
    private static void appendInList(StringBuilder sql, List<Object> params, String col, String op,
                                     String rawList, String fieldType, ZoneId zoneId) {
        List<Object> values = parseList(rawList);
        if (values.isEmpty()) {
            sql.append(op.equals("not in") ? "1=1" : "1=0");
            return;
        }

        if (isDateType(fieldType)) {
            List<Object> conv = new ArrayList<>(values.size());
            for (Object v : values) {
                ParsedTemporal pt = parseTemporal(String.valueOf(v), zoneId);
                if (pt == null) continue;
                conv.add(java.sql.Timestamp.from(pt.instant));
            }
            if (conv.isEmpty()) { sql.append(op.equals("not in") ? "1=1" : "1=0"); return; }
            sql.append(col).append(" ").append(op.toUpperCase(Locale.ROOT)).append(" (")
                    .append(SqlStringUtil.nPlaceholders(conv.size())).append(")");
            params.addAll(conv);
            return;
        }

        sql.append(col).append(" ").append(op.toUpperCase(Locale.ROOT)).append(" (")
           .append(SqlStringUtil.nPlaceholders(values.size())).append(")");
        params.addAll(values);
    }

    private static final class ParsedTemporal {
        final boolean isDateOnly;
        final Instant instant;
        final Instant startInstant;
        final Instant endExclusiveInstant;
        ParsedTemporal(boolean dateOnly, Instant instant, Instant start, Instant endExcl) {
            this.isDateOnly = dateOnly;
            this.instant = instant;
            this.startInstant = start;
            this.endExclusiveInstant = endExcl;
        }
    }

    private static ParsedTemporal parseTemporal(String token, ZoneId zoneId) {
        if (token == null) return null;
        String t = token.trim();
        if (t.startsWith("'") && t.endsWith("'")) t = t.substring(1, t.length() - 1);

        if (t.matches("\\d{4}-\\d{2}-\\d{2}$")) {
            try {
                LocalDate d = LocalDate.parse(t, DateTimeFormatter.ISO_LOCAL_DATE);
                ZonedDateTime zStart = d.atStartOfDay(zoneId);
                ZonedDateTime zNext  = d.plusDays(1).atStartOfDay(zoneId);
                return new ParsedTemporal(true, null, zStart.toInstant(), zNext.toInstant());
            } catch (DateTimeParseException ignored) { return null; }
        }

        try {
            OffsetDateTime odt = OffsetDateTime.parse(t, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return new ParsedTemporal(false, odt.toInstant(), null, null);
        } catch (DateTimeParseException ignore) { /* try next */ }

        try {
            String norm = t.replace(' ', 'T');
            LocalDateTime ldt = LocalDateTime.parse(norm, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return new ParsedTemporal(false, ldt.atZone(zoneId).toInstant(), null, null);
        } catch (DateTimeParseException ignore) { /* try next */ }

        try {
            Instant inst = Instant.parse(t);
            return new ParsedTemporal(false, inst, null, null);
        } catch (DateTimeParseException ignore) { }

        return null;
    }

    private static void appendSimpleComparison(StringBuilder sql, List<Object> params, String col, String op, String rawValue) {

    	String sym;
        
        switch (op) {
        
            case "eq": 
            	sym = "="; 
            	break;
            	
            case "ne": 
            	sym = "<>"; 
            	break;
            	
            case "gt": 
            	sym = ">"; 
            	break;
            	
            case "ge": 
            	sym = ">="; 
            	break;
            	
            case "lt": 
            	sym = "<"; 
            	break;
            	
            case "le": 
            	sym = "<="; 
            	break;
            
            case "like": 
            	sym = "LIKE"; 
            	break;
            	
            default: 
            	sym = "=";
        }
        
        sql.append(col).append(" ").append(sym).append(" ?");
        params.add(parseScalar(rawValue));
    }

    private static void appendLikeVariant(StringBuilder sql, List<Object> params, String col, String variant, String rawValue) {
        String v = asString(parseScalar(rawValue));
        switch (variant) {

        	case "contains":
            	v = "%" + v + "%";
            	break;
            
        	case "startswith":
        		v = v + "%"; 
        		break;
        		
            case "endswith":   
            	v = "%" + v; 
            	break;
        }
        sql.append("LOWER(").append(col).append(") LIKE LOWER(?)");
        params.add(v);
    }

    private static Object parseScalar(String token) {
        if (token == null) return null;
        String t = token.trim();
        if (t.startsWith("'") && t.endsWith("'")) {
            return t.substring(1, t.length() - 1);
        }
        try { return Long.parseLong(t); } catch (NumberFormatException ignore) {}
        return t;
    }

    private static List<Object> parseList(String token) {
        if (token == null) return Collections.emptyList();
        String t = token.trim();
        if (!t.startsWith("(") || !t.endsWith(")")) return Collections.emptyList();
        String inner = t.substring(1, t.length() - 1).trim();
        if (inner.isEmpty()) return Collections.emptyList();

        List<Object> values = new ArrayList<>();
        for (String part : inner.split(",")) {
            String p = part.trim();
            values.add(parseScalar(p));
        }
        return values;
    }

    private static String group(Matcher m, String name) {
        String s = m.group(name);
        return (s == null) ? null : s.trim();
    }

    private static String asString(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
