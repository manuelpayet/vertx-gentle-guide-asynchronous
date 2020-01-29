package io.vertx.guides.wiki;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.guides.wiki.database.DatabaseConstants;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.AuthInitializerVerticle;
import io.vertx.guides.wiki.http.HttpServerVerticle;

@RunWith(VertxUnitRunner.class)
public class ApiTest {

	private Vertx vertx;
	private WebClient webClient;

	@Before
	public void prepare(TestContext context) {
		vertx = Vertx.vertx();

		final JsonObject databaseConf = new JsonObject()
				.put(DatabaseConstants.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
				.put(DatabaseConstants.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);

		Promise<String> authInitializedPromise = Promise.promise();

		vertx.deployVerticle(new AuthInitializerVerticle(), new DeploymentOptions().setConfig(databaseConf),
				authInitializedPromise);

		Future<String> wikiDatabasePromise = authInitializedPromise.future().compose((t) -> {
			final Promise<String> wikiDeployedPromise = Promise.promise();
			vertx.deployVerticle(new WikiDatabaseVerticle(), new DeploymentOptions().setConfig(databaseConf), wikiDeployedPromise);
			return wikiDeployedPromise.future();
		});

		
		wikiDatabasePromise.compose((t) -> {
			Promise<String> httpServerVerticle = Promise.promise();
			vertx.deployVerticle(new HttpServerVerticle(), httpServerVerticle);
			return httpServerVerticle.future();
		}).setHandler(context.asyncAssertSuccess());
		
		
		webClient = WebClient.create(vertx,
				new WebClientOptions().setDefaultHost("localhost").setSsl(true)
						.setTrustOptions(new JksOptions().setPath("server-keystore.jks").setPassword("secret"))
						.setDefaultPort(8080));

	}

	@Test
	public void playWithApi(TestContext context) {
		Async async = context.async();

		JsonObject page = new JsonObject().put("name", "Sample").put("markdown", "# A page");

		Promise<String> tokenPromise = Promise.promise();
		webClient.get("/api/token").as(BodyCodec.string())
		.putHeader("login", "root")
		.putHeader("password", "admin")
		.send((response) -> {
			context.put("jwtToken", String.format("Bearer %s", response.result().body()));
			tokenPromise.complete();
		});
		
		
		final Future<String> postPageFuture = tokenPromise.future().compose((t) -> {
			Promise<String> postPagePromise = Promise.promise();
			System.err.println("jwt token" + context.get("jwtToken"));
			webClient.post("/api/pages").as(BodyCodec.jsonObject())
					.putHeader("Authorization", context.get("jwtToken"))
					.expect(ResponsePredicate.SC_SUCCESS)
					.sendJsonObject(page, (res) -> postPagePromise.complete());
			return postPagePromise.future();
		});
		

		Future<HttpResponse<JsonObject>> getPageFuture = postPageFuture.compose(resp -> {
			Promise<HttpResponse<JsonObject>> promise = Promise.promise();
			webClient.get("/api/pages").as(BodyCodec.jsonObject()).putHeader("Authorization", context.get("jwtToken")).expect(ResponsePredicate.SC_SUCCESS).send(promise);
			return promise.future();
		});

		Future<HttpResponse<JsonObject>> updatePageFuture = getPageFuture.compose(resp -> {
			JsonArray array = resp.body().getJsonArray("pages");
			context.assertEquals(1, array.size());
			context.assertEquals(0, array.getJsonObject(0).getInteger("id"));
			Promise<HttpResponse<JsonObject>> promise = Promise.promise();
			JsonObject data = new JsonObject().put("id", 0).put("markdown", "Oh Yeah!");
			webClient.put("/api/pages/0").as(BodyCodec.jsonObject()).putHeader("Authorization", context.get("jwtToken")).expect(ResponsePredicate.SC_SUCCESS)
					.sendJsonObject(data, promise);
			return promise.future();
		});

		Future<HttpResponse<JsonObject>> deletePageFuture = updatePageFuture.compose(resp -> {
			context.assertTrue(resp.body().getBoolean("success"));
			Promise<HttpResponse<JsonObject>> promise = Promise.promise();
			webClient.delete("/api/pages/0").as(BodyCodec.jsonObject()).putHeader("Authorization", context.get("jwtToken")).expect(ResponsePredicate.SC_SUCCESS)
					.send(promise);
			return promise.future();
		});

		deletePageFuture.setHandler(ar -> {
			if (ar.succeeded()) {
				context.assertTrue(ar.result().body().getBoolean("success"));
				async.complete();
			} else {
				context.fail(ar.cause());
			}
		});

		async.awaitSuccess(5000);
	}

	@After
	public void close(TestContext context) {
		vertx.close(context.asyncAssertSuccess());
	}

}
