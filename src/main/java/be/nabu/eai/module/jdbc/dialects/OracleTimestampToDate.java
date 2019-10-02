package be.nabu.eai.module.jdbc.dialects;

import java.sql.SQLException;
import java.util.Date;

import be.nabu.libs.converter.api.ConverterProvider;
import oracle.sql.TIMESTAMP;

public class OracleTimestampToDate implements ConverterProvider<TIMESTAMP, Date> {

	@Override
	public Date convert(TIMESTAMP instance) {
		try {
			return instance == null ? null : instance.timestampValue();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Class<TIMESTAMP> getSourceClass() {
		return TIMESTAMP.class;
	}

	@Override
	public Class<Date> getTargetClass() {
		return Date.class;
	}

}
