package be.nabu.eai.module.jdbc.dialects;

import be.nabu.libs.converter.api.ConverterProvider;
import oracle.sql.TIMESTAMP;

public class OracleTimestampToString implements ConverterProvider<TIMESTAMP, String> {

	@Override
	public String convert(TIMESTAMP instance) {
		return instance == null ? null : instance.stringValue();
	}

	@Override
	public Class<TIMESTAMP> getSourceClass() {
		return TIMESTAMP.class;
	}

	@Override
	public Class<String> getTargetClass() {
		return String.class;
	}

}
