package io.vertx.guides.wiki.http;

import static io.vertx.guides.wiki.database.DatabaseConstants.CONFIG_WIKIDB_JDBC_DRIVER_CLASS;
import static io.vertx.guides.wiki.database.DatabaseConstants.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE;
import static io.vertx.guides.wiki.database.DatabaseConstants.CONFIG_WIKIDB_JDBC_URL;
import static io.vertx.guides.wiki.database.DatabaseConstants.DEFAULT_JDBC_MAX_POOL_SIZE;
import static io.vertx.guides.wiki.database.DatabaseConstants.DEFAULT_WIKIDB_JDBC_DRIVER_CLASS;
import static io.vertx.guides.wiki.database.DatabaseConstants.DEFAULT_WIKIDB_JDBC_URL;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.netty.handler.codec.http.HttpStatusClass;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.auth.jdbc.JDBCAuth;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CookieHandler;
import io.vertx.ext.web.handler.FormLoginHandler;
import io.vertx.ext.web.handler.RedirectAuthHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.UserSessionHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;
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

		JDBCClient client = JDBCClient.createShared(vertx,
				new JsonObject().put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, DEFAULT_WIKIDB_JDBC_URL))
						.put("driver_class",
								config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, DEFAULT_WIKIDB_JDBC_DRIVER_CLASS))
						.put("max_pool_size",
								config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, DEFAULT_JDBC_MAX_POOL_SIZE)));

		JDBCAuth auth = JDBCAuth.create(vertx, client);

		HttpServer server = vertx.createHttpServer(new HttpServerOptions().setSsl(true)
				.setKeyStoreOptions(new JksOptions().setPath("server-keystore.jks").setPassword("secret")));

		Router router = Router.router(vertx);

		router.route().handler(BodyHandler.create());
		router.route().handler(SessionHandler.create(LocalSessionStore.create(vertx)));
		router.route().handler(UserSessionHandler.create(auth));

		AuthHandler authHandler = RedirectAuthHandler.create(auth, "/login");
		router.route("/").handler(authHandler);
		router.route("/wiki/*").handler(authHandler);
		router.route("/action/*").handler(authHandler);

		router.get("/").handler(this::indexHandler);
		router.get("/wiki/:page").handler(this::pageRenderingHandler);
		router.post().handler(BodyHandler.create());
		router.post("/save").handler(this::pageUpdateHandler);
		router.post("/create").handler(this::pageCreateHandler);
		router.post("/delete").handler(this::pageDeletionHandler);
		router.get("/backup").handler(this::backupHandler);

		router.get("/login").handler(this::loginHandler);
		router.post("/login-auth").handler(FormLoginHandler.create(auth));

		router.get("/logout").handler(context -> {

			context.clearUser();
			context.response().setStatusCode(302).putHeader("Location", "/").end();
		});

		Router apiRouter = Router.router(vertx);
		apiRouter.get("/pages").handler(this::apiRoot);
		apiRouter.get("/pages/:id").handler(this::apiGetPage);
		apiRouter.post().handler(BodyHandler.create());
		apiRouter.post("/pages").handler(this::apiCreatePage);
		apiRouter.put().handler(BodyHandler.create());
		apiRouter.put("/pages/:id").handler(this::apiUpdatePage);
		apiRouter.delete("/pages/:id").handler(this::apiDeletePage);
		router.mountSubRouter("/api", apiRouter);

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
		webClient = WebClient.create(vertx, new WebClientOptions().setSsl(true).setUserAgent("vert-x3"));

	}

	private void loginHandler(RoutingContext context) {
		context.put("title", "Login");
		freeMarkerTemplateEngine.render(context.data(), "templates/login.ftl", ar -> {
			if (ar.succeeded()) {
				context.response().putHeader("Content-Type", "text/html");
				context.response().end(ar.result());
			} else {
				LOGGER.error("could not render login page", ar.cause());
				context.fail(ar.cause());
			}
		});
	}

	private void indexHandler(RoutingContext context) {
		context.user().isAuthorized("create", res -> {
			boolean canCreatePage = res.succeeded() && res.result();
			dbService.fetchAllPages(reply -> {
				LOGGER.info("response received from Database for indexHandler");
				if (reply.succeeded()) {
					context.put("title", "Wiki Home");
					context.put("pages", reply.result().getList());
					context.put("canCreatePage", canCreatePage);
					context.put("username", context.user().principal().getString("username"));
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
			if (reply.succeeded()) {
				JsonArray filesObject = new JsonArray();

				JsonObject payload = new JsonObject().put("files", filesObject).put("language", "plaintext")
						.put("title", "vertx-wiki-backup").put("public", true);

				reply.result().forEach(page -> {
					filesObject.add(new JsonObject().put("name", page.getString("NAME")).put("content",
							page.getString("CONTENT")));
				});

				webClient.post(443, "snippets.glot.io", "/snippets").putHeader("Content-Type", "application/json")
						.as(BodyCodec.jsonObject()).sendJsonObject(payload, postBackup -> {
							LOGGER.info(String.format("reponse : %s", postBackup.result().body()));
							if (postBackup.succeeded()) {
								final HttpResponse<JsonObject> responsePost = postBackup.result();
								JsonObject body = responsePost.body();
								if (HttpStatusClass.SUCCESS.contains(responsePost.statusCode())) {
									String url = String.format("https://glot.io/snippets/%s", body.getString("id"));
									context.put("backup_gist_url", url);
									indexHandler(context);
								} else {
									final StringBuilder message = new StringBuilder();
									message.append("Could not backup the wiki: ");
									if (!body.isEmpty()) {
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
		context.user().isAuthorized("delete", isDeletableResultHandler -> {
			if (isDeletableResultHandler.succeeded() && isDeletableResultHandler.result()) {
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
			} else {
				context.response().setStatusCode(403).end();
			}
		});
	}

	private void apiRoot(RoutingContext context) {
		dbService.fetchAllPagesData(allPagesDataHandler -> {
			if (allPagesDataHandler.succeeded()) {
				List<JsonObject> pages = allPagesDataHandler.result().stream().map(element -> new JsonObject()
						.put("id", element.getInteger("ID")).put("name", element.getString("NAME")))
						.collect(Collectors.toList());
				final JsonObject responseOK = new JsonObject().put("success", true).put("pages", pages);
				context.response().setStatusCode(200).putHeader("Content-Type", "application/json")
						.end(responseOK.encode());

			} else {
				final JsonObject responseFailed = new JsonObject().put("success", false).put("error",
						"allPagesDataHandler.cause().getMessage()");
				context.response().setStatusCode(500).putHeader("Content-Type", "application/json")
						.end(responseFailed.encode());
				LOGGER.error("API: could not fetch all pages data from database", allPagesDataHandler.cause());
			}
		});
	}

	private void apiGetPage(RoutingContext context) {
		int id = Integer.valueOf(context.request().getParam("id"));
		dbService.fetchPageById(id, reply -> {
			JsonObject response = new JsonObject();
			if (reply.succeeded()) {
				JsonObject dbObject = reply.result();
				if (dbObject.getBoolean("found")) {
					JsonObject payload = new JsonObject().put("name", dbObject.getString("name"))
							.put("id", dbObject.getInteger("id")).put("markdown", dbObject.getString("content"))
							.put("html", Processor.process(dbObject.getString("content")));
					response.put("success", true).put("page", payload);
					context.response().setStatusCode(200);
				} else {
					context.response().setStatusCode(404);
					response.put("success", false).put("error", "There is no page with ID " + id);
				}
			} else {
				response.put("success", false).put("error", reply.cause().getMessage());
				context.response().setStatusCode(500);
			}
			context.response().putHeader("Content-Type", "application/json");
			context.response().end(response.encode());
		});
	}

	private void apiCreatePage(RoutingContext context) {
		JsonObject page = context.getBodyAsJson();
		if (!validateJsonPageDocument(context, page, "name", "markdown")) {
			return;
		}
		dbService.createPage(page.getString("name"), page.getString("markdown"), reply -> {
			if (reply.succeeded()) {
				context.response().setStatusCode(201);
				context.response().putHeader("Content-Type", "application/json");
				context.response().end(new JsonObject().put("success", true).encode());
			} else {
				context.response().setStatusCode(500);
				context.response().putHeader("Content-Type", "application/json");
				context.response()
						.end(new JsonObject().put("success", false).put("error", reply.cause().getMessage()).encode());
			}
		});
	}

	private void apiUpdatePage(RoutingContext context) {
		int id = Integer.valueOf(context.request().getParam("id"));
		JsonObject page = context.getBodyAsJson();
		if (!validateJsonPageDocument(context, page, "markdown")) {
			return;
		}
		dbService.savePage(id, page.getString("markdown"), reply -> {
			handleSimpleDbReply(context, reply);
		});
	}

	private void apiDeletePage(RoutingContext context) {
		int id = Integer.valueOf(context.request().getParam("id"));
		dbService.deletePage(id, reply -> {
			handleSimpleDbReply(context, reply);
		});
	}

	private void handleSimpleDbReply(RoutingContext context, AsyncResult<Void> reply) {
		if (reply.succeeded()) {
			context.response().setStatusCode(200);
			context.response().putHeader("Content-Type", "application/json");
			context.response().end(new JsonObject().put("success", true).encode());
		} else {
			context.response().setStatusCode(500);
			context.response().putHeader("Content-Type", "application/json");
			context.response()
					.end(new JsonObject().put("success", false).put("error", reply.cause().getMessage()).encode());
		}
	}

	private boolean validateJsonPageDocument(RoutingContext context, JsonObject page, String... expectedKeys) {
		if (!Arrays.stream(expectedKeys).allMatch(page::containsKey)) {
			LOGGER.error("Bad page creation JSON payload: " + page.encodePrettily() + " from "
					+ context.request().remoteAddress());
			context.response().setStatusCode(400);
			context.response().putHeader("Content-Type", "application/json");
			context.response().end(new JsonObject().put("success", false).put("error", "Bad request payload").encode());
			return false;
		}
		return true;
	}

}
