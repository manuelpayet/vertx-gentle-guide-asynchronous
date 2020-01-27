package io.vertx.guides.wiki.http;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.netty.handler.codec.http.HttpStatusClass;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;
import io.vertx.guides.wiki.database.WikiDatabaseService;

public class HttpServerVerticle extends AbstractVerticle {
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

	public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
	public static final String CONFIG_WIKI_DB_QUEUE = "wikidb.queue";

	private String wikiDbQueue = "wikidb.queue";
	private WikiDatabaseService dbService;
	
	private FreeMarkerTemplateEngine freeMarkerTemplateEngine;

	private WebClient webClient;
	
	@Override
	public void start(Promise<Void> promise) throws Exception {
		wikiDbQueue = config().getString(CONFIG_WIKI_DB_QUEUE, "wikidb.queue");
		dbService = WikiDatabaseService.createProxy(vertx, wikiDbQueue);
		
		HttpServer server = vertx.createHttpServer();
		Router router = Router.router(vertx);
		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler);
		router.post("/delete").handler(this::pageDeletionHandler);
		router.get("/backup").handler(this::backupHandler);
		
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
		webClient = WebClient.create(vertx, new WebClientOptions()
											.setSsl(true)
											.setUserAgent("vert-x3"));
		
	}

	private void indexHandler(RoutingContext context) {
		dbService.fetchAllPages(reply -> {
			LOGGER.info("response received from Database for indexHandler");
			if (reply.succeeded()) {
				context.put("title", "Wiki Home");
				context.put("pages", reply.result().getList());
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
		dbService.fetchPage(requestedPage, reply -> {

			if (reply.succeeded()) {
				JsonObject body = (JsonObject) reply.result();

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
		
		String id = context.request().getParam("id");
		String markdown = context.request().getParam("markdown");

	
		Handler<AsyncResult<Void>> handler = reply -> {
			if (reply.succeeded()) {
				context.response().setStatusCode(303);
				context.response().putHeader("Location", "/wiki/" + title);
				context.response().end();
			} else {
				context.fail(reply.cause());
			}
		};
		
		
		if ("yes".equals(context.request().getParam("newPage"))) {
			dbService.createPage(title, markdown, handler);
		} else {
			dbService.savePage(Integer.valueOf(id), markdown, handler);
		}

		
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
	
	private void backupHandler(RoutingContext context) {
		LOGGER.info("in backupHandler");
		dbService.fetchAllPagesData(reply -> {
			if(reply.succeeded()) {
				JsonArray filesObject = new JsonArray();
				
				JsonObject payload = new JsonObject()
						.put("files", filesObject)
						.put("language", "plaintext")
						.put("title", "vertx-wiki-backup")
						.put("public", true);
				
				reply
					.result()
					.forEach(page -> {
						filesObject.add(new JsonObject()
											.put("name", page.getString("NAME"))
											.put("content", page.getString("CONTENT"))
										);
					});
				
				webClient
					.post(443, "snippets.glot.io", "/snippets")
					.putHeader("Content-Type", "application/json")
					.as(BodyCodec.jsonObject())
					.sendJsonObject(payload, postBackup -> {
						LOGGER.info(String.format("reponse : %s", postBackup.result().body()));
						if(postBackup.succeeded()) {
							final HttpResponse<JsonObject> responsePost = postBackup.result();
							JsonObject body = responsePost.body();
							if(HttpStatusClass.SUCCESS.contains(responsePost.statusCode())) {
								String url = String.format("https://glot.io/snippets/%s", body.getString("id"));
								context.put("backup_gist_url", url);
								indexHandler(context);
							} else {
								final StringBuilder message = new StringBuilder();
								message.append("Could not backup the wiki: ");
								if(!body.isEmpty()) {
									message.append(System.getProperty("line.separator"))
									.append(body.encodePrettily());
								}
								LOGGER.error(message.toString());
								context.fail(502);
							}
						} else {
							LOGGER.error("could not post snippet to Glot.io", postBackup.cause());
							context.fail(postBackup.cause());
						}
					});
				
			} else {
				LOGGER.error("failed at fetching data to backup");
				context.fail(reply.cause());
			}
		});
	}

	private void pageDeletionHandler(RoutingContext context) {
		String id = context.request().getParam("id");
		dbService.deletePage(Integer.valueOf(id), reply -> {
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
