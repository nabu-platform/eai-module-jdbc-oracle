/*
* Copyright (C) 2016 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
