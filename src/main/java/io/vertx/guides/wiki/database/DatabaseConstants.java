package io.vertx.guides.wiki.database;

public interface DatabaseConstants {

	String CONFIG_WIKIDB_QUEUE = "wikidb.queue";
	String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
	String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
	String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
	String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";
	
	String DEFAULT_WIKIDB_JDBC_URL = "jdbc:hsqldb:file:db/wiki";
	int DEFAULT_JDBC_MAX_POOL_SIZE = 30;
	String DEFAULT_WIKIDB_JDBC_DRIVER_CLASS = "org.hsqldb.jdbcDriver";

}