package io.vertx.guides.wiki.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.serviceproxy.ServiceBinder;

import static io.vertx.guides.wiki.database.DatabaseConstants.*;

public class WikiDatabaseVerticle extends AbstractVerticle {

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

	private Map<SqlQuery, String> sqlQueries;

	private void loadSqlQueries() throws IOException {
		String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE,
				"./src/main/resources/db-queries.properties"); //TODO: dirty: will only work in IDE, will not work when packaged
		Properties queriesProp = new Properties();
		LOGGER.info("Absolute path of queries property file : " + new File(queriesFile).getAbsolutePath());
		try (InputStream queriesInputStream = new FileInputStream(queriesFile)) {
			queriesProp.load(queriesInputStream);
		}
		sqlQueries = Arrays.stream(SqlQuery.values())
				.collect(Collectors.toMap(Function.identity(), query -> queriesProp.getProperty(query.getKey())));
	}

	private JDBCClient dbClient;

	@Override
	public void start(Promise<Void> promise) throws Exception {
		loadSqlQueries();

		dbClient = JDBCClient.createShared(vertx,
				new JsonObject().put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, DEFAULT_WIKIDB_JDBC_URL))
						.put("driver_class",
								config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, DEFAULT_WIKIDB_JDBC_DRIVER_CLASS))
						.put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, DEFAULT_JDBC_MAX_POOL_SIZE)));
		WikiDatabaseService.create(dbClient, sqlQueries, ready -> {
			if(ready.succeeded()) {
				ServiceBinder binder = new ServiceBinder(vertx);
				binder
				.setAddress(CONFIG_WIKIDB_QUEUE)
				.register(WikiDatabaseService.class, ready.result());
				promise.complete();
			} else {
				LOGGER.error("impossible to initialize WikiDatabaseService", ready.cause());
				promise.fail(ready.cause());
			}
		});

	}

	private enum ActionsDispatcher {
		ALL_PAGES("all-pages", WikiDatabaseVerticle::fetchAllPages),
		GET_PAGE("get-page", WikiDatabaseVerticle::fetchPage),
		CREATE_PAGE("create-page", WikiDatabaseVerticle::createPage),
		SAVE_PAGE("save-page", WikiDatabaseVerticle::savePage),
		DELETE_PAGE("delete-page", WikiDatabaseVerticle::deletePage),;

		private final String action;
		private final BiConsumer<WikiDatabaseVerticle, Message<JsonObject>> handler;

		ActionsDispatcher(final String action, BiConsumer<WikiDatabaseVerticle, Message<JsonObject>> handler) {
			this.action = action;
			this.handler = handler;
		}

		public String getAction() {
			return action;
		}

		public void executeHandler(WikiDatabaseVerticle instance, Message<JsonObject> message) {
			handler.accept(instance, message);
		}
	}

	public void onMessage(Message<JsonObject> message) {
		LOGGER.info(String.format("message received for database with message %s and headers %s", message.body(), message.headers()));
		if (!message.headers().contains("action")) {
			LOGGER.error("No action specified in header");
			message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
		}

		final String actionType = message.headers().get("action");
		LOGGER.info("action dispatched is " + Arrays.stream(ActionsDispatcher.values()).filter(action -> actionType.equals(action.getAction())).findFirst());
		Arrays.stream(ActionsDispatcher.values()).filter(action -> actionType.equals(action.getAction())).findFirst()
				.ifPresentOrElse(a -> a.executeHandler(this, message), () -> this.badAction(message));

	}

	public void reportQueryError(final Message<JsonObject> message, Throwable cause) {
		LOGGER.error("Database query error", cause);
		message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
	}

	public void fetchAllPages(Message<JsonObject> message) {
		LOGGER.info("received request to fetch all pages");
		dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
			if (res.succeeded()) {
				final List<String> pageNamesFromDb = res.result().getResults().stream()
						.map(resultLine -> resultLine.getString(0)).sorted().collect(Collectors.toList());
				message.reply(new JsonObject().put("pages", new JsonArray(pageNamesFromDb)));
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}

	public void fetchPage(Message<JsonObject> message) {
		final String requestedPage = message.body().getString("page");
		final JsonArray queryParam = new JsonArray().add(requestedPage);
		dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), queryParam, res -> {
			if (res.succeeded()) {
				final JsonObject responseMessage = res.result().getResults().stream().findFirst()
						.map(firstResult -> new JsonObject().put("found", true).put("id", firstResult.getInteger(0))
								.put("rawContent", firstResult.getString(1)))
						.orElse(new JsonObject().put("found", false));
				message.reply(responseMessage);
			} else {
				reportQueryError(message, res.cause());
			}
		});

	}

	public void createPage(Message<JsonObject> message) {
		JsonArray data = new JsonArray().add(message.body().getString("title"))
				.add(message.body().getString("markdown"));
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}

	public void savePage(Message<JsonObject> message) {
		JsonArray data = new JsonArray().add(message.body().getString("markdown")).add(message.body().getInteger("id"));
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}

	public void deletePage(Message<JsonObject> message) {
		final JsonArray data = new JsonArray().add(message.body().getString("id"));
		dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
			if (res.succeeded()) {
				message.reply("ok");
			} else {
				reportQueryError(message, res.cause());
			}
		});
	}

	public void badAction(Message<JsonObject> message) {
		message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + message.headers().get("action"));
	}

}
