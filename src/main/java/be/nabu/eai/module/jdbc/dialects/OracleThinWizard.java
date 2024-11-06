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

import be.nabu.eai.module.jdbc.pool.JDBCPoolArtifact;
import be.nabu.eai.module.jdbc.pool.api.JDBCPoolWizard;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.resources.RepositoryEntry;

public class OracleThinWizard implements JDBCPoolWizard<OracleThinParameters> {

	@Override
	public String getIcon() {
		return "oracle-icon.png";
	}

	@Override
	public String getName() {
		return "Oracle (thin)";
	}

	@Override
	public Class<OracleThinParameters> getWizardClass() {
		return OracleThinParameters.class;
	}

	@Override
	public OracleThinParameters load(JDBCPoolArtifact pool) {
		String jdbcUrl = pool.getConfig().getJdbcUrl();
		if (jdbcUrl != null && jdbcUrl.startsWith("jdbc:oracle:thin:@")) {
			OracleThinParameters parameters = new OracleThinParameters();
			try {
				// either jdbc:oracle:thin:@prodHost:1521:ORCL (where ORCL is the system identifier = sid)
				// or jdbc:oracle:thin:@prodHost:1521/ORCL where ORCL is the database name
				// additional leading // can be added like jdbc:oracle:thin:@//myHost:1521/service_name
				String [] parts = jdbcUrl.substring("jdbc:oracle:thin:@".length()).replaceAll("^[/]+", "").split(":");
				if (parts.length >= 2) {
					parameters.setHost(parts[0]);
					if (parts.length == 2) {
						String [] subParts = parts[1].split("/");
						parameters.setPort(Integer.parseInt(subParts[0]));
						parameters.setDatabase(subParts[1]);
					}
					else {
						parameters.setPort(Integer.parseInt(parts[1]));
						parameters.setSystemIdentifier(parts[2]);
					}
					parameters.setUsername(pool.getConfig().getUsername());
					parameters.setPassword(pool.getConfig().getPassword());
					return parameters;
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public JDBCPoolArtifact apply(Entry project, RepositoryEntry entry, OracleThinParameters properties, boolean isNew, boolean isMain) {
		try {
			JDBCPoolArtifact existing = isNew ? new JDBCPoolArtifact(entry.getId(), entry.getContainer(), entry.getRepository()) : (JDBCPoolArtifact) entry.getNode().getArtifact();
			if (isNew) {
				existing.getConfig().setAutoCommit(false);
			}
			String jdbcUrl = "jdbc:oracle:thin:@" + (properties.getHost() == null ? "localhost" : properties.getHost()) + ":" + (properties.getPort() == null ? 1521 : properties.getPort());
			if (properties.getDatabase() != null) {
				jdbcUrl += "/" + properties.getDatabase();
			}
			else if (properties.getSystemIdentifier() != null) {
				jdbcUrl += ":" + properties.getSystemIdentifier();
			}
			existing.getConfig().setJdbcUrl(jdbcUrl); 
			Class clazz = Oracle.class;
			existing.getConfig().setDialect(clazz);
			existing.getConfig().setDriverClassName("oracle.jdbc.OracleDriver");
			return existing;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
