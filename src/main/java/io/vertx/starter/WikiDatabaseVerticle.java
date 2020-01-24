package io.vertx.starter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.ObjectUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;

public class WikiDatabaseVerticle extends AbstractVerticle {
	public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
	public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
	public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
	public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";

	public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

	private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

	private enum SqlQuery {

		CREATE_PAGES_TABLE("create-pages-table"), ALL_PAGES("all-pages"), GET_PAGE("get-page"),
		CREATE_PAGE("create-page"), SAVE_PAGE("save-page"), DELETE_PAGE("delete-page");

		private final String key;

		private SqlQuery(final String key) {
			this.key = key;
		}

		public String getKey() {
			return key;
		}

	}

	private Map<SqlQuery, String> sqlQueries;
 
	private void loadSqlQueries() throws IOException {
		String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE, "./src/main/resources/db-queries.properties");
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
				new JsonObject().put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
						.put("driver_class",
								config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
						.put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

		dbClient.getConnection(ar -> {
			if (ar.failed()) {
				LOGGER.error("could not open a database connection", ar.cause());
				promise.fail(ar.cause());
			} else {
				LOGGER.info("WikiDatabase Verticle initialized ");
				vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage);
				promise.complete();
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

		ActionsDispatcher(final String action, final BiConsumer<WikiDatabaseVerticle, Message<JsonObject>> handler) {
			this.action = action;
			this.handler = handler;
		}

		public String getAction() {
			return action;
		}

		public BiConsumer<WikiDatabaseVerticle, Message<JsonObject>> getHandler() {
			return handler;
		}

	}

	public enum ErrorCodes {
		NO_ACTION_SPECIFIED, BAD_ACTION, DB_ERROR
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
				.map(ActionsDispatcher::getHandler).orElseGet(this::badAction)
				.accept(this, message);

	}

	private void reportQueryError(final Message<JsonObject> message, Throwable cause) {
		LOGGER.error("Database query error", cause);
		message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
	}

	private static void fetchAllPages(WikiDatabaseVerticle instance, Message<JsonObject> message) {
		LOGGER.info("received request to fetch all pages");
		instance.dbClient.query(instance.sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
			if (res.succeeded()) {
				final List<String> pageNamesFromDb = res.result().getResults().stream()
				.map(resultLine -> resultLine.getString(0))
				.sorted()
				.collect(Collectors.toList());
				message.reply(new JsonObject().put("pages", new JsonArray(pageNamesFromDb)));
			} else {
				instance.reportQueryError(message, res.cause());
			}
		});
	}

	private static void fetchPage(WikiDatabaseVerticle instance, Message<JsonObject> message) {
		final String requestedPage = message.body().getString("page");
		final JsonArray queryParam = new JsonArray().add(requestedPage);
		instance.dbClient.queryWithParams(instance.sqlQueries.get(SqlQuery.GET_PAGE), queryParam, res -> {
			if (res.succeeded()) {
				final JsonObject responseMessage =  res.result().getResults().stream()
				.findFirst()
				.map(firstResult -> 
					new JsonObject()
					.put("found", true)
					.put("id", firstResult.getInteger(0))
					.put("rawContent", firstResult.getString(1)))
				.orElse(new JsonObject().put("found", false));
				message.reply(responseMessage);
			} else {
				instance.reportQueryError(message, res.cause());
			}
		});
	

	}

	private static void createPage(WikiDatabaseVerticle instance, Message<JsonObject> message) {
		JsonArray data = new JsonArray()
								.add(message.body().getString("title"))
								.add(message.body().getString("markdown"));
		instance.dbClient.updateWithParams(instance.sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
			if(res.succeeded()) {
				message.reply("ok");
			} else {
				instance.reportQueryError(message, res.cause());
			}
		});
	}

	private static void savePage(WikiDatabaseVerticle instance, Message<JsonObject> message) {
		JsonArray data = new JsonArray()
							.add(message.body().getString("markdown"))
							.add(message.body().getInteger("id"));
		instance.dbClient.updateWithParams(instance.sqlQueries.get(SqlQuery.SAVE_PAGE), data,  res -> {
			if(res.succeeded()) {
				message.reply("ok");
			} else {
				instance.reportQueryError(message, res.cause());
			}
		});
	}

	private static void deletePage(WikiDatabaseVerticle instance, Message<JsonObject> message) {
		final JsonArray data = new JsonArray()
								.add(message.body().getString("id"));
		instance.dbClient.updateWithParams(instance.sqlQueries.get(SqlQuery.DELETE_PAGE), data,  res -> {
			if(res.succeeded()) {
				message.reply("ok");
			} else {
				instance.reportQueryError(message, res.cause());
			}
		});
	}

	public BiConsumer<WikiDatabaseVerticle, Message<JsonObject>> badAction() {
		return new BiConsumer<WikiDatabaseVerticle, Message<JsonObject>>() {
			@Override
			public void accept(WikiDatabaseVerticle instance, Message<JsonObject> message) {
				message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + message.headers().get("action"));
			}
		};
		
	}

}
