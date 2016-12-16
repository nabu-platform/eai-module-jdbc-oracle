Example configuration:

```xml
<jdbcPool>
    <driverClassName>oracle.jdbc.OracleDriver</driverClassName>
    <jdbcUrl>jdbc:oracle:thin:@localhost:7002:delphi</jdbcUrl>
    <username>eoly</username>
    <password>eoly</password>
    <maximumPoolSize>10</maximumPoolSize>
    <minimumIdle>5</minimumIdle>
    <autoCommit>false</autoCommit>
    <dialect>be.nabu.eai.module.jdbc.oracle.dialects.Oracle</dialect>
    <enableMetrics>true</enableMetrics>
</jdbcPool>
```

Be sure to use ``oracle.jdbc.OracleDriver``, not the ``oracle.jdbc.driver.OracleDriver`` one, it is deprecated.