package io.vertx.guides.wiki.database;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

public class WikiDatabaseServiceImpl implements WikiDatabaseService {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseServiceImpl.class);
	
	private final JDBCClient dbClient;
	private final Map<SqlQuery, String> sqlQueries;

	
	
	public WikiDatabaseServiceImpl(JDBCClient dbClient, Map<SqlQuery, String> sqlQueries, Handler<AsyncResult<WikiDatabaseService>> readyHandler) {
		this.dbClient = dbClient;
		this.sqlQueries = sqlQueries;
		
		dbClient.getConnection(ar -> {
			if(ar.succeeded()) {
				SQLConnection sqlConnection = ar.result();
				sqlConnection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), createHandler -> {
					sqlConnection.close();
					if(createHandler.succeeded()) {
						readyHandler.handle(Future.succeededFuture(this));
					} else {
						LOGGER.error("Database preparation error", createHandler.cause());
						readyHandler.handle(Future.failedFuture(createHandler.cause()));
					}
				});
			} else {
				LOGGER.error("Could not open a database connection", ar.cause());
				readyHandler.handle(Future.failedFuture(ar.cause()));
			}
		});
	}

	@Override
	public WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler) {
		LOGGER.info("received request to fetch all pages");
		dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
			if (res.succeeded()) {
				final List<String> pageNamesFromDb = res.result().getResults().stream()
						.map(resultLine -> resultLine.getString(0)).sorted().collect(Collectors.toList());
				resultHandler.handle(Future.succeededFuture(new JsonArray(pageNamesFromDb)));
			} else {
				LOGGER.error("Database query error", res.cause());
				resultHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		return this;
	}

	@Override
	public WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler) {
		final JsonArray queryParam = new JsonArray().add(name);
		dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), queryParam, res -> {
			if (res.succeeded()) {
				final JsonObject responseMessage = res.result().getResults().stream().findFirst()
						.map(firstResult -> new JsonObject().put("found", true).put("id", firstResult.getInteger(0))
								.put("rawContent", firstResult.getString(1)))
						.orElse(new JsonObject().put("found", false));
				resultHandler.handle(Future.succeededFuture(responseMessage));
			} else {
				LOGGER.error("Database query error", res.cause());
				resultHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		return this;
	}

	@Override
	public WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler) {
		JsonArray data = new JsonArray().add(title)
				.add(markdown);
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
			if (res.succeeded()) {
				resultHandler.handle(Future.succeededFuture());
			} else {
				LOGGER.error("Database insert error", res.cause());
				resultHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		return this;
	}

	@Override
	public WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler) {
		JsonArray data = new JsonArray().add(markdown).add(id);
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
			if (res.succeeded()) {
				resultHandler.handle(Future.succeededFuture());
			} else {
				LOGGER.error("Database save page ", res.cause());
				resultHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		return this;
	}

	@Override
	public WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler) {
		final JsonArray data = new JsonArray().add(id);
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
			if (res.succeeded()) {
				resultHandler.handle(Future.succeededFuture());
			} else {
				LOGGER.error("failed to delete message", res.cause());
				resultHandler.handle(Future.failedFuture(res.cause()));
			}
		});
		return this;
	}

}
