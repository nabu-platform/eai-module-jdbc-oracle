package be.nabu.eai.module.jdbc.oracle.dialects;

import java.io.IOException;
import java.io.Writer;
import java.math.BigInteger;
import java.net.URI;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import oracle.jdbc.OracleConnection;
import be.nabu.eai.module.jdbc.dialects.PostgreSQL;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.converter.api.Converter;
import be.nabu.libs.evaluator.QueryParser;
import be.nabu.libs.evaluator.QueryPart;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.jdbc.api.SQLDialect;
import be.nabu.libs.types.DefinedTypeResolverFactory;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.FormatProperty;
import be.nabu.libs.types.properties.MaxLengthProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.properties.UniqueProperty;

public class Oracle implements SQLDialect {

	@Override
	public boolean hasArraySupport(Element<?> element) {
		return false;
	}

	@Override
	public String getSQLName(Class<?> instanceClass) {
		String sqlName = SQLDialect.super.getSQLName(instanceClass);
		return sqlName == null || !sqlName.equals("varchar") ? sqlName : "varchar2";
	}
	
	private String getArraySQLName(Element<?> element) {
		String sqlName = SQLDialect.super.getSQLName(((SimpleType<?>) element.getType()).getInstanceClass());
		return sqlName == null || !sqlName.equals("varchar") ? sqlName : "varchar2(255)";
	}
	
	@Override
	public Class<?> getTargetClass(Class<?> clazz) {
		// UUID is not a dedicated type in oracle
		if (UUID.class.isAssignableFrom(clazz)) {
			return String.class;
		}
		// make sure we transform booleans to integers
		else if (Boolean.class.isAssignableFrom(clazz)) {
			return Integer.class;
		}
		return SQLDialect.super.getTargetClass(clazz);
	}

	@Override
	public Integer getSQLType(Class<?> instanceClass) {
		if (Boolean.class.equals(instanceClass)) {
			return Types.NUMERIC;
		}
		else if (UUID.class.equals(instanceClass)) {
			return Types.VARCHAR;
		}
		else {
			return SQLDialect.super.getSQLType(instanceClass);
		}
	}

	@Override
	public void setArray(PreparedStatement statement, Element<?> element, int index, Collection<?> collection) throws SQLException {
		String sqlName = getArraySQLName(element);
		if (sqlName == null) {
			throw new IllegalArgumentException("Could not determine the oracle sql name of: " + element.getName());
		}
		if (collection.isEmpty()) {
			statement.setNull(index, getSQLType(element), sqlName);
		}
		else {
			Connection connection = statement.getConnection();
			if (!(connection instanceof OracleConnection)) {
				connection = connection.unwrap(OracleConnection.class);
			}
			String typeName = (EAIRepositoryUtils.uncamelify(element.getName()) + "_array").toUpperCase();
//			Statement create = statement.getConnection().createStatement();
//			create.execute("create type " + typeName + " is varray(" + collection.size() + ") of " + sqlName);
			Array array = ((OracleConnection) connection).createOracleArray(typeName, collection.toArray());
			statement.setArray(index, array);
		}
		// this is deprecated, presumably since 11.2.0.5.0 when the above stuff was added
//		ARRAY array = oracleConnection.createARRAY(getSQLName(element), collection.toArray());
//		((OraclePreparedStatement) statement).setARRAY(index, array);
	}
	
	@Override
	public String rewrite(String sql, ComplexType input, ComplexType output) {
		// rewrite booleans to integers
		// perhaps too broad...
		sql = sql.replaceAll("\\btrue\\b", "1");
		sql = sql.replaceAll("\\bfalse\\b", "0");
		
		// we have a merge statement
		if (sql.matches("(?i)(?s)[\\s]*\\binsert into\\b.*\\bon conflict\\b.*\\bdo update\\b.*")) {
			try {
				sql = rewriteMerge(sql);
			}
			catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}
		return sql;
	}
	
	public static void main(String...args) throws ParseException {
		System.out.println(rewriteMerge("insert into delivery_point_calculation (\n" + 
				"	id,\n" + 
				"	created,\n" + 
				"	modified,\n" + 
				"	slice_start_date,\n" + 
				"	slice_end_date,\n" + 
				"	consumption,\n" + 
				"	consumption_unit_id,\n" + 
				"	cost,\n" + 
				"	currency_id,\n" + 
				"	insight_delivery_point_id,\n" + 
				"	invoice_id\n" + 
				") values (\n" + 
				"	:id,\n" + 
				"	:created,\n" + 
				"	:modified,\n" + 
				"	:sliceStartDate,\n" + 
				"	:sliceEndDate,\n" + 
				"	:consumption,\n" + 
				"	:consumptionUnitId,\n" + 
				"	:cost,\n" + 
				"	:currencyId,\n" + 
				"	:insightDeliveryPointId,\n" + 
				"	:invoiceId\n" + 
				")\n" + 
				"on conflict (insight_delivery_point_id, invoice_id, slice_start_date) do update\n" + 
				"set modified= excluded.modified, consumption= excluded.consumption, consumption_unit_id= excluded.consumptionUnitId, cost= excluded.cost, currency_id= excluded.currencyId, slice_end_date= excluded.sliceEndDate"));
	}
	/**
	 * Example:
	 * 
	 * INSERT INTO user_logins (username, logins) VALUES ('Naomi',1),('James',1)
	 * 		ON CONFLICT (username)
	 * 		DO UPDATE SET logins = user_logins.logins + EXCLUDED.logins;
	 */
	public static String rewriteMerge(String sql) throws ParseException {
		List<QueryPart> parsed = QueryParser.getInstance().interpret(QueryParser.getInstance().tokenize(sql), true);
		int counter = 0;
		if (!validate(parsed, counter++, "insert") || !validate(parsed, counter++, "into")) {
			throw new ParseException("Expecint 'insert into'", counter);
		}
		String table = parsed.get(counter++).getToken().getContent();
		System.out.println("Table: " + table);
		// target table
		String tableAlias = "tt";
		System.out.println("Alias: " + tableAlias);
		if (!validate(parsed, counter++, "(")) {
			throw new ParseException("Expecting opening '(' to list the fields", counter);
		}
		List<String> fields = new ArrayList<String>();
		while (counter < parsed.size()) {
			if (validate(parsed, counter, ")")) {
				counter++;
				break;
			}
			// need a separator for more fields
			else if (!fields.isEmpty() && !validate(parsed, counter++, ",")) {
				throw new ParseException("Expecting either a ',' to separate the fields or a ')' to stop them", counter);
			}
			fields.add(parsed.get(counter++).getToken().getContent());
		}
		System.out.println("Fields: " + fields);
		if (!validate(parsed, counter++, "values")) {
			throw new ParseException("Expecting fixed string 'values' indicating start of values", counter);
		}
		List<List<String>> values = new ArrayList<List<String>>();
		List<String> current = null;
		boolean isNamed = false;
		while (counter < parsed.size()) {
			// we start a new value sequence
			if (validate(parsed, counter, "(")) {
				if (current != null) {
					throw new ParseException("List of values not closed properly", counter);
				}
				counter++;
				current = new ArrayList<String>();
				values.add(current);
			}
			else if (validate(parsed, counter, ")")) {
				counter++;
				current = null;
			}
			else if (validate(parsed, counter, ",")) {
				if (values.isEmpty()) {
					throw new ParseException("Unexpected value list separator", counter);
				}
				counter++;
			}
			else if (validate(parsed, counter, ":")) {
				isNamed = true;
				counter++;
			}
			// if we don't have a current value list and we are encountering other tokens, we have exited the value listing
			else if (current == null) {
				break;
			}
			else {
				if (isNamed) {
					current.add(":" + parsed.get(counter++).getToken().getContent());
					isNamed = false;
				}
				else {
					current.add(parsed.get(counter++).getToken().getContent());
				}
			}
		}
		System.out.println("Values: " + values);
		if (!validate(parsed, counter++, "on") || !validate(parsed, counter++, "conflict")) {
			throw new ParseException("Expecting 'on conflict'", counter);
		}
		if (!validate(parsed, counter++, "(")) {
			throw new ParseException("Expecting brackets around the conflicted fields", counter);
		}
		List<String> conflicts = new ArrayList<String>();
		while (counter < parsed.size()) {
			if (validate(parsed, counter, ")")) {
				counter++;
				break;
			}
			// need a separator for more fields
			else if (!conflicts.isEmpty() && !validate(parsed, counter++, ",")) {
				throw new ParseException("Expecting either a ',' to separate the conflicted fields or a ')' to stop them", counter);
			}
			String conflict = parsed.get(counter++).getToken().getContent();
			if (!fields.contains(conflict)) {
				throw new ParseException("The conflicted field '" + conflict + "' is not in the field list that is inserted", counter);
			}
			conflicts.add(conflict);
		}
		System.out.println("Conflicts: " + conflicts);
		if (!validate(parsed, counter++, "do") || !validate(parsed, counter++, "update") || !validate(parsed, counter++, "set")) {
			throw new ParseException("Expecting 'do update set'", counter);
		}
		// the rest of the update statement can be copied verbatim
		StringBuilder updateStatement = new StringBuilder();
		while (counter < parsed.size()) {
			String content = parsed.get(counter++).getToken().getContent();
			if (!content.equals(",")) {
				updateStatement.append(" ");
			}
			// if we are referencing the original table, inject the table alias
			if (fields.contains(content)) {
				updateStatement.append(tableAlias).append(".").append(content);	
			}
			else {
				updateStatement.append(content);
			}
		}
		StringBuilder result = new StringBuilder();
		result.append("merge into ")
			.append(table)
			.append(" ")
			.append(tableAlias)
			.append("\n\tusing (");
		
		for (int i = 0; i < values.size(); i++) {
			if (i == 0) {
				result.append("select ");
			}
			else {
				result.append(" union all select ");
			}
			for (int j = 0; j < values.get(i).size(); j++) {
				if (j > 0) {
					result.append(", ");
				}
				result.append(values.get(i).get(j))
					.append(" as ")
					.append(fields.get(j));
			}
			result.append(" from dual");
		}
		
		result.append(") excluded\n\ton (");
		for (int i = 0; i < conflicts.size(); i++) {
			if (i > 0) {
				result.append(" and ");
			}
			result.append(tableAlias)
				.append(".")
				.append(conflicts.get(i))
				.append(" = excluded.")
				.append(conflicts.get(i));
		}
		result.append(")\n\twhen matched then update set")
			.append(updateStatement)
			.append("\n\twhen not matched then insert (");
		for (int i = 0; i < fields.size(); i++) {
			if (i > 0) {
				result.append(", ");
			}
			result.append(tableAlias).append(".").append(fields.get(i));
		}
		result.append(") values (");
		for (int i = 0; i < fields.size(); i++) {
			if (i > 0) {
				result.append(", ");
			}
			result.append("excluded.").append(fields.get(i));
		}
		result.append(")");
		return result.toString();
	}
	
	private static boolean validate(List<QueryPart> tokens, int offset, String value) {
		return tokens.get(offset).getToken().getContent().toLowerCase().equals(value.toLowerCase());
	}
	
	@Override
	public String limit(String sql, Integer offset, Integer limit) {
		if (limit != null) {
			if (offset == null) {
				offset = 0;
			}
			sql = "select results.*, rownum as record_number from (" + sql + ") results where rownum <= " + (offset + limit);
			if (offset > 0) {
				sql = "select results.* from (" + sql + ") results where record_number >= " + offset;
			}
		}
		return sql;
	}

	@Override
	public String buildCreateSQL(ComplexType type) {
		StringBuilder builder = new StringBuilder();
		builder.append("create table " + EAIRepositoryUtils.uncamelify(PostgreSQL.getName(type.getProperties())) + " (\n");
		boolean first = true;
		StringBuilder constraints = new StringBuilder();
		for (Element<?> child : TypeUtils.getAllChildren(type)) {
			if (first) {
				first = false;
			}
			else {
				builder.append(",\n");
			}
			// if we have a complex type, generate an id field that references it
			if (child.getType() instanceof ComplexType) {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName()) + "_id uuid");
			}
			// differentiate between dates
			else if (Date.class.isAssignableFrom(((SimpleType<?>) child.getType()).getInstanceClass())) {
				Value<String> property = child.getProperty(FormatProperty.getInstance());
				String format = property == null ? "dateTime" : property.getValue();
				if (format.equals("dateTime")) {
					format = "timestamp";
				}
				else if (!format.equals("date") && !format.equals("time")) {
					format = "timestamp";
				}
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ").append(format);
			}
			else {
				builder.append("\t" + EAIRepositoryUtils.uncamelify(child.getName())).append(" ")
					.append(getPredefinedSQLType(child));
			}
			
			Value<String> foreignKey = child.getProperty(ForeignKeyProperty.getInstance());
			if (foreignKey != null) {
				String[] split = foreignKey.getValue().split(":");
				if (split.length == 2) {
					if (!constraints.toString().isEmpty()) {
						constraints.append(",\n");
					}
					DefinedType resolve = DefinedTypeResolverFactory.getInstance().getResolver().resolve(split[0]);
					String referencedName = ValueUtils.getValue(CollectionNameProperty.getInstance(), resolve.getProperties());
					if (referencedName == null) {
						referencedName = resolve.getName();
					}
					constraints.append("\tforeign key (" +  EAIRepositoryUtils.uncamelify(child.getName()) + ") references " + EAIRepositoryUtils.uncamelify(referencedName) + "(" + split[1] + ")");
				}
			}
			
			if (child.getName().equals("id")) {
				builder.append(" primary key");
			}
			else {
				Integer value = ValueUtils.getValue(MinOccursProperty.getInstance(), child.getProperties());
				if (value == null || value > 0) {
					builder.append(" not null");
				}
			}
			
			Value<Boolean> property = child.getProperty(UniqueProperty.getInstance());
			if (property != null && property.getValue()) {
				if (!constraints.toString().isEmpty()) {
					constraints.append(",\n");
				}
				constraints.append("\tconstraint " + EAIRepositoryUtils.uncamelify(child.getName()) + "_unique unique (" + child.getName() + ")");
			}
		}
		if (!constraints.toString().isEmpty()) {
			builder.append(",\n").append(constraints.toString());
		}
		builder.append("\n);");
		return builder.toString();
	}
	
	private String getPredefinedSQLType(Element<?> element) {
		Class<?> instanceClass = ((SimpleType<?>) element.getType()).getInstanceClass();
		if (String.class.equals(instanceClass) || char[].class.equals(instanceClass)) {
			Integer sqlType = getSQLType(element);
			if (sqlType == Types.CLOB) {
				return "clob";
			}
		}
		return getPredefinedSQLType(instanceClass);
	}
	
	private static String getPredefinedSQLType(Class<?> instanceClass) {
		if (String.class.isAssignableFrom(instanceClass) || char[].class.isAssignableFrom(instanceClass) || URI.class.isAssignableFrom(instanceClass) || instanceClass.isEnum()) {
			return "varchar2(4000)";
		}
		else if (byte[].class.isAssignableFrom(instanceClass)) {
			return "varbinary";
		}
		else if (Integer.class.isAssignableFrom(instanceClass)) {
			return "integer";
		}
		else if (Long.class.isAssignableFrom(instanceClass)) {
			return "longinteger";
		}
		else if (BigInteger.class.isAssignableFrom(instanceClass)) {
			return "number(*, 0)";
		}
		else if (Double.class.isAssignableFrom(instanceClass)) {
			return "number";
		}
		else if (Float.class.isAssignableFrom(instanceClass)) {
			return "number";
		}
		else if (Short.class.isAssignableFrom(instanceClass)) {
			return "shortinteger";
		}
		else if (Boolean.class.isAssignableFrom(instanceClass)) {
			return "number(1, 0)";
		}
		else if (UUID.class.isAssignableFrom(instanceClass)) {
			return "varchar2(36)";
		}
		else if (Date.class.isAssignableFrom(instanceClass)) {
			return "timestamp";
		}
		else {
			return null;
		}
	}

	@Override
	public String buildInsertSQL(ComplexContent content) {
		StringBuilder keyBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		SimpleDateFormat timestampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		timestampFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = new Date();
		for (Element<?> element : TypeUtils.getAllChildren(content.getType())) {
			if (element.getType() instanceof SimpleType) {
				Class<?> instanceClass = ((SimpleType<?>) element.getType()).getInstanceClass();
				if (!keyBuilder.toString().isEmpty()) {
					keyBuilder.append(",\n\t");
					valueBuilder.append(",\n\t");
				}
				keyBuilder.append(EAIRepositoryUtils.uncamelify(element.getName()));
				Object value = content.get(element.getName());
				Integer minOccurs = ValueUtils.getValue(MinOccursProperty.getInstance(), element.getProperties());
				// if there is no value but it is mandatory, try to generate one
				if (value == null && minOccurs != null && minOccurs > 0) {
					if (UUID.class.isAssignableFrom(instanceClass)) {
						value = UUID.randomUUID();
					}
					else if (Date.class.isAssignableFrom(instanceClass)) {
						value = date;
					}
					else if (Number.class.isAssignableFrom(instanceClass)) {
						value = 0;
					}
					else if (Boolean.class.isAssignableFrom(instanceClass)) {
						value = false;
					}
				}
				if (value == null) {
					valueBuilder.append("null");
				}
				else {
					boolean closeQuote = false;
					if (Boolean.class.isAssignableFrom(instanceClass)) {
						if ((Boolean) value) {
							valueBuilder.append("1");
						}
						else {
							valueBuilder.append("0");
						}
					}
					else if (Date.class.isAssignableFrom(instanceClass)) {
						Value<String> property = element.getProperty(FormatProperty.getInstance());
						if (property != null && !property.getValue().equals("timestamp") && !property.getValue().contains("S") && !property.getValue().equals("time")) {
							valueBuilder.append("to_timestamp('").append(timestampFormatter.format(value)).append("', 'yyyy-mm-dd hh24:mi:ss.ff3')");
						}
						else {
							valueBuilder.append("to_date('").append(dateFormatter.format(value)).append("', 'yyyy-mm-dd hh24:mi:ss')");
						}
					}
					else {
						if (URI.class.isAssignableFrom(instanceClass) || String.class.isAssignableFrom(instanceClass) || UUID.class.isAssignableFrom(instanceClass)) {
							valueBuilder.append("'");
							closeQuote = true;
						}
						valueBuilder.append(value.toString());
						if (closeQuote) {
							valueBuilder.append("'");
						}
					}
				}
			}
		}
		return "insert into " + EAIRepositoryUtils.uncamelify(PostgreSQL.getName(content.getType().getProperties())) + " (\n\t" + keyBuilder.toString() + "\n) values (\n\t" + valueBuilder.toString() + "\n);";
	}

	@Override
	public void setObject(PreparedStatement statement, Element<?> element, int index, Object value) throws SQLException, ServiceException {
		SimpleType<?> type = (SimpleType<?>) element.getType();
		boolean set = false;
		// check for clobs
		if (String.class.isAssignableFrom(type.getInstanceClass())) {
			Value<Integer> property = element.getProperty(MaxLengthProperty.getInstance());
			// make it a clob
			if (property != null && property.getValue() > 4000) {
				set = true;
				if (value == null) {
					statement.setNull(index, Types.CLOB);
				}
				else {
					Converter converter = ConverterFactory.getInstance().getConverter();
					Clob clob = statement.getConnection().createClob();
					// TODO 0-based or 1-based?
					Writer writer = clob.setCharacterStream(1);
					try {
						writer.write(value instanceof String ? (String) value : converter.convert(value, String.class));
						writer.flush();
					}
					catch (IOException e) {
						throw new ServiceException("ORACLE-1", "Can not write clob stream", e);
					}
					statement.setClob(index, clob);
				}
			}
		}
		if (!set) {
			SQLDialect.super.setObject(statement, element, index, value);
		}
	}

	@Override
	public Integer getSQLType(Element<?> element) {
		SimpleType<?> type = (SimpleType<?>) element.getType();
		if (String.class.isAssignableFrom(type.getInstanceClass()) || char[].class.isAssignableFrom(type.getInstanceClass())) {
			Value<Integer> property = element.getProperty(MaxLengthProperty.getInstance());
			if (property != null && property.getValue() > 4000) {
				return Types.CLOB;
			}
		}
		return SQLDialect.super.getSQLType(element);
	}
	
	
}