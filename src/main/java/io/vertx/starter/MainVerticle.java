package io.vertx.starter;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.netty.util.internal.StringUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

public class MainVerticle extends AbstractVerticle {
	private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
	private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
	private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
	private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
	private static final String SQL_ALL_PAGES = "select Name from Pages";
	private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

	private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

	private JDBCClient dbClient;

	private Future<Void> prepareDatabase() {
		Promise<Void> promise = Promise.promise();
		dbClient = JDBCClient.createShared(vertx, new JsonObject().put("url", "jdbc:hsqldb:file:db/wiki")
				.put("driver_class", "org.hsqldb.jdbcDriver").put("max_pool_size", 30));

		dbClient.getConnection(ar -> {
			if (ar.failed()) {
				LOGGER.error("Could not open a database connection", ar.cause());
				promise.fail(ar.cause());
			} else {
				SQLConnection connection = ar.result();
				connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
					connection.close();
					if (create.failed()) {
						LOGGER.error("Database preparation error", create.cause());
						promise.fail(create.cause());
					} else {
						promise.complete();
					}
				});
			}
		});

		return promise.future();
	}

	private FreeMarkerTemplateEngine templateEngine;
	private Future<Void> startHttpServer() {
		Promise<Void> promise = Promise.promise();
		HttpServer server = vertx.createHttpServer();

		Router router = Router.router(vertx); 
		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler); 
		router.post("/delete").handler(this::pageDeletionHandler);

		templateEngine = FreeMarkerTemplateEngine.create(vertx);
		
		server.requestHandler(router)
		.listen(8080, ar -> {
			if(ar.succeeded()) {
				LOGGER.info("HTTP Server running on Port 8080");
				promise.complete();
			} else {
				LOGGER.error("Could not start HTTP Server", ar.cause());
				promise.fail(ar.cause());
			}
		});
		
		return promise.future();
	}
	
	
	private void indexHandler(RoutingContext context) {
		dbClient.getConnection(car -> {
			if(car.succeeded()) {
				SQLConnection connection = car.result();
				connection.query(SQL_ALL_PAGES, res -> {
					connection.close();
					if(res.succeeded()) {
						List<String> pages = res.result()
								.getResults()
								.stream()
								.map(json -> json.getString(0))
								.sorted()
								.collect(Collectors.toList());
						context.put("title", "Wiki home");
						context.put("pages", pages);
						templateEngine.render(context.data(), "templates/index.ftl", ar -> {
							if(ar.succeeded()) {
								context.response().putHeader("Content-Type", "text/html");
								context.response().end(ar.result());
							} else {
								context.fail(ar.cause());
							}
						});
					} else {
						context.fail(res.cause());
					}
				});
			}
		});
	}
	
	private static final String EMPTY_PAGE_MARKDOWN = 
			  "# A new page\n" +
					    "\n" +
					    "Feel-free to write in Markdown!\n";

	private void pageRenderingHandler(RoutingContext context) {
		String page = context.request().getParam("page");
		
		dbClient.getConnection(car -> {
			if(car.succeeded()) {
				SQLConnection connection = car.result();
				connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
					connection.close();
					if(fetch.succeeded()) {
						JsonArray row = fetch.result().getResults()
								.stream()
								.findFirst()
								.orElse(new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
						Integer id = row.getInteger(0);
						String rawContent = row.getString(1);
						
						context.put("title", page);
						context.put("id", id);
						context.put("newPage", fetch.result().getResults().size()==0?"yes":"no");
						context.put("rawContent", rawContent);
						context.put("content", Processor.process(rawContent));
						context.put("timestamp", new Date().toString());
						
						templateEngine.render(context.data(), "templates/page.ftl" , ar -> {
							if(ar.succeeded()) {
								context.response().putHeader("Content-Type", "text/html");
								context.response().end(ar.result());
							} else {
								context.fail(ar.cause());
							}
						});
					} else {
						context.fail(fetch.cause());
					}
				});
			} else {
				context.fail(car.cause());
			}
		});
	}
	
	private void pageCreateHandler(RoutingContext context) {
		String pageName = context.request().getParam("name"); //comes from POST form
		String location = "wiki/" + pageName;
		if(StringUtil.isNullOrEmpty(pageName)) {
			location = "/";
		}
		context.response().setStatusCode(303);
		context.response().putHeader("Location", location);
		context.response().end();
		
	}
	
	
	private void pageUpdateHandler(RoutingContext context) {
		String id = context.request().getParam("id");
		String title = context.request().getParam("title");
		String markdown = context.request().getParam("markdown");
		boolean newPage = "yes".equals(context.request().getParam("newPage"));
		
		dbClient.getConnection(car -> {
			if(car.succeeded()) {
				SQLConnection connection = car.result();
				final String sql;
				JsonArray jsonArray = new JsonArray();
				if(newPage) {
					sql = SQL_CREATE_PAGE;
					jsonArray.add(title).add(markdown);
				} else {
					sql = SQL_SAVE_PAGE;
					jsonArray.add(markdown).add(id);
				}
				connection.updateWithParams(sql, jsonArray, res -> {
					if(res.succeeded()) {
						context.response().setStatusCode(303);
						context.response().putHeader("Location", "/wiki/" + title);
						context.response().end();
					} else {
						context.fail(res.cause());
					}
				});
				connection.close();
			} else {
				context.fail(car.cause());
			}
		});
	}
	
	private void pageDeletionHandler(RoutingContext context) {
		String id = context.request().getParam("id");
		dbClient.getConnection(car -> {
			if(car.succeeded()) {
				SQLConnection sqlConnection = car.result();
				sqlConnection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
					if(res.succeeded()) {
						context.response().setStatusCode(303);
						context.response().putHeader("Location", "/");
						context.response().end();
					} else {
						context.fail(res.cause());
					}
				});
				sqlConnection.close();
			} else {
				context.fail(car.cause());
			}
		});	
	}
	
	
	
	@Override
	public void start(Promise<Void> promise) throws Exception {
		Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
		steps.setHandler(promise);
	}

}
