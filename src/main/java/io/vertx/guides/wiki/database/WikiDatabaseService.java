package io.vertx.guides.wiki.database;

import java.util.List;
import java.util.Map;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.jdbc.JDBCClient;

@ProxyGen
@VertxGen
public interface WikiDatabaseService {
	@Fluent
	WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler);

	@Fluent
	WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler);
	
	@Fluent
	WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler);

	@Fluent
	WikiDatabaseService fetchPageById(int id, Handler<AsyncResult<JsonObject>> resultHandler);
	
	@Fluent
	WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler);

	@Fluent
	WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler);

	@Fluent
	WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler);

	@GenIgnore
	static WikiDatabaseService create(JDBCClient dbClient, Map<SqlQuery, String> sqlQueries, Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
	  return new WikiDatabaseServiceImpl(dbClient, sqlQueries, readyHandler);
	}
	
	
	@GenIgnore
	static WikiDatabaseService createProxy(Vertx vertx, String address) {
		return new WikiDatabaseServiceVertxEBProxy(vertx, address);
	}
}
