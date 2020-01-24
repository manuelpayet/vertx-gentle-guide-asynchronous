package io.vertx.starter;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.netty.util.internal.StringUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

public class HttpServerVerticle extends AbstractVerticle {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

	public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
	public static final String CONFIG_WIKI_DB_QUEUE = "wikidb.queue";

	private String wikiDbQueue = "wikidb.queue";

	private FreeMarkerTemplateEngine freeMarkerTemplateEngine;

	@Override
	public void start(Promise<Void> promise) throws Exception {
		wikiDbQueue = config().getString(CONFIG_WIKI_DB_QUEUE, "wikidb.queue");

		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);
		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler);
		router.post("/delete").handler(this::pageDeletionHandler);

		freeMarkerTemplateEngine = FreeMarkerTemplateEngine.create(vertx);

		int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
		server.requestHandler(router).listen(portNumber, ar -> {
			if (ar.succeeded()) {
				LOGGER.info("HTTP server running on port " + portNumber);
				promise.complete();
			} else {
				LOGGER.error("Could not start HTTP server", ar.cause());
				promise.fail(ar.cause());
			}
		});
	}

	private void indexHandler(RoutingContext context) {
		DeliveryOptions options = new DeliveryOptions().addHeader("action", "all-pages");
		vertx.eventBus().request(wikiDbQueue, new JsonObject(), options, reply -> {
			LOGGER.info("response received from Database for indexHandler");
			if (reply.succeeded()) {
				JsonObject body = (JsonObject) reply.result().body();
				context.put("title", "Wiki Home");
				context.put("pages", body.getJsonArray("pages").getList());
				freeMarkerTemplateEngine.render(context.data(), "templates/index.ftl", ar -> {
					if (ar.succeeded()) {
						context.response().putHeader("Content-Type", "text/html");
						context.response().end(ar.result());
					} else {
						LOGGER.error("indexHandler Error", ar.cause());
						context.fail(ar.cause());
					}
				});
			} else {
				LOGGER.error("indexHandler Error", reply.cause());
				context.fail(reply.cause());
			}
		});
	}

	private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n" + "\n" + "Feel-free to write in Markdown!\n";

	private void pageRenderingHandler(RoutingContext context) {

		String requestedPage = context.request().getParam("page");
		JsonObject request = new JsonObject().put("page", requestedPage);

		DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-page");
		vertx.eventBus().request(wikiDbQueue, request, options, reply -> {

			if (reply.succeeded()) {
				JsonObject body = (JsonObject) reply.result().body();

				boolean found = body.getBoolean("found");
				String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);
				context.put("title", requestedPage);
				context.put("id", body.getInteger("id", -1));
				context.put("newPage", found ? "no" : "yes");
				context.put("rawContent", rawContent);
				context.put("content", Processor.process(rawContent));
				context.put("timestamp", new Date().toString());
 
				freeMarkerTemplateEngine.render(context.data(), "templates/page.ftl", ar -> {
					if (ar.succeeded()) {
						context.response().putHeader("Content-Type", "text/html");
						context.response().end(ar.result());
					} else {
						context.fail(ar.cause());
					}
				});

			} else {
				context.fail(reply.cause());
			}
		});
	}

	private void pageUpdateHandler(RoutingContext context) {

		String title = context.request().getParam("title");
		JsonObject request = new JsonObject().put("id", context.request().getParam("id")).put("title", title)
				.put("markdown", context.request().getParam("markdown"));

		DeliveryOptions options = new DeliveryOptions();
		if ("yes".equals(context.request().getParam("newPage"))) {
			options.addHeader("action", "create-page");
		} else {
			options.addHeader("action", "save-page");
		}

		vertx.eventBus().request(wikiDbQueue, request, options, reply -> {
			if (reply.succeeded()) {
				context.response().setStatusCode(303);
				context.response().putHeader("Location", "/wiki/" + title);
				context.response().end();
			} else {
				context.fail(reply.cause());
			}
		});
	}

	private void pageCreateHandler(RoutingContext context) {
		String pageName = context.request().getParam("name");
		String location = "/wiki/" + pageName;
		if (pageName == null || pageName.isEmpty()) {
			location = "/";
		}
		context.response().setStatusCode(303);
		context.response().putHeader("Location", location);
		context.response().end();
	}

	private void pageDeletionHandler(RoutingContext context) {
		String id = context.request().getParam("id");
		JsonObject request = new JsonObject().put("id", id);
		DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-page");
		vertx.eventBus().request(wikiDbQueue, request, options, reply -> {
			if (reply.succeeded()) {
				context.response().setStatusCode(303);
				context.response().putHeader("Location", "/");
				context.response().end();
			} else {
				context.fail(reply.cause());
			}
		});
	}

}
