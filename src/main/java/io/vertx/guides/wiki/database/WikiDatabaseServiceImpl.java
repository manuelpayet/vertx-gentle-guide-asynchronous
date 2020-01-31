package io.vertx.guides.wiki.database;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.reactivex.SingleHelper;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.reactivex.ext.sql.SQLConnection;

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
		
		dbClient.rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES))
				.flatMapPublisher(res -> {
					List<JsonArray> results = res.getResults();
					return Flowable.fromIterable(results);
				}).map(json -> json.getString(0))
				.sorted()
				.collect(JsonArray::new, JsonArray::add)
				.subscribe(SingleHelper.toObserver(resultHandler));
		
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

	@Override
	public WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler) {
		dbClient.rxQuery(sqlQueries.get(SqlQuery.ALL_PAGES_DATA))
				.map(ResultSet::getRows)
				.subscribe(SingleHelper.toObserver(resultHandler));
		return this;
	}

	@Override
	public WikiDatabaseService fetchPageById(int id, Handler<AsyncResult<JsonObject>> resultHandler) {
		dbClient.rxQueryWithParams(sqlQueries.get(SqlQuery.GET_PAGE_BY_ID), new JsonArray().add(id))
		.subscribe((result) -> {
			final List<JsonObject> results = result.getRows();
			final Optional<JsonObject> pageData = results.stream().findFirst();
			final JsonObject payload = pageData.map(r -> new JsonObject()
														.put("found", true)
														.put("id", r.getInteger("ID"))
														.put("name", r.getString("NAME"))
														.put("content", r.getString("CONTENT")))
														.orElse(
																new JsonObject()
																	.put("found", false)
																	.put("id", id)
														);
			resultHandler.handle(Future.succeededFuture(payload));
		}, (exception) -> {
			LOGGER.error("could not fetch by id", exception);
			resultHandler.handle(Future.failedFuture(exception));
		});
		
		dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE_BY_ID), new JsonArray().add(id), fetchByIdHandler -> {
			if(fetchByIdHandler.succeeded()) {
				
				
			} else {
				
			}
		});
		return this;
	}

}
