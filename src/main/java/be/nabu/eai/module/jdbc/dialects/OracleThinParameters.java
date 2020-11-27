package be.nabu.eai.module.jdbc.dialects;

import be.nabu.libs.types.api.annotation.ComplexTypeDescriptor;
import be.nabu.libs.types.api.annotation.Field;

@ComplexTypeDescriptor(propOrder = { "host", "port", "systemIdentifier", "database", "username", "password" })
public class OracleThinParameters {
	private String host, database, systemIdentifier;
	private Integer port;
	private String username, password;
	
	@Field(defaultValue = "localhost")
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	
	@Field(defaultValue = "1521")
	public Integer getPort() {
		return port;
	}
	public void setPort(Integer port) {
		this.port = port;
	}
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}

	@Field(hide = "systemIdentifier != null")
	public String getDatabase() {
		return database;
	}
	public void setDatabase(String database) {
		this.database = database;
	}
	
	@Field(hide = "database != null")
	public String getSystemIdentifier() {
		return systemIdentifier;
	}
	public void setSystemIdentifier(String systemIdentifier) {
		this.systemIdentifier = systemIdentifier;
	}

	
}
