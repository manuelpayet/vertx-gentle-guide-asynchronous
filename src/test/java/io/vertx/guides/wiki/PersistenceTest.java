package io.vertx.guides.wiki;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.guides.wiki.database.DatabaseConstants;
import io.vertx.guides.wiki.database.WikiDatabaseService;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.HttpServerVerticle;

@RunWith(VertxUnitRunner.class)
public class PersistenceTest {
	private Vertx vertx;
	private WikiDatabaseService dbService;
	
	
	
	
	@Before
	public void prepare(TestContext context) throws InterruptedException {
		vertx = Vertx.vertx();
		
		final JsonObject databaseConf = new JsonObject()
											.put(DatabaseConstants.CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:mem:testdb;shutdown=true")
											.put(DatabaseConstants.CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 4);
		
		vertx.deployVerticle(new WikiDatabaseVerticle(), new DeploymentOptions().setConfig(databaseConf), 
															context.asyncAssertSuccess(result -> 
															dbService = WikiDatabaseService.createProxy(vertx, DatabaseConstants.CONFIG_WIKIDB_QUEUE)));

	}
	
	@Test(timeout = 3000)
	public void async_behaviour(TestContext context) {
		Vertx vertx = Vertx.vertx(); //we are in a test, we have to create the vertx context ourserlves
		context.assertEquals("foo", "foo");
		Async asyncCallAtLeastOnce = context.async();
		Async asyncCallAtLeastThrice = context.async(3);
		
		vertx.setTimer(100, handler -> asyncCallAtLeastOnce.complete());
		vertx.setPeriodic(100, handler-> asyncCallAtLeastThrice.countDown());
	}
	
	@Test
	public void crudOperations(final TestContext context) {
		Async async = context.async();
		dbService.createPage("Test", "Some content", context.asyncAssertSuccess(createdPageJsonContent -> {
			dbService.fetchPage("Test", context.asyncAssertSuccess(fetchedPageJsonContent -> {
				context.assertTrue(fetchedPageJsonContent.getBoolean("found"));
				context.assertTrue(fetchedPageJsonContent.containsKey("id"));
				context.assertEquals("Some content", fetchedPageJsonContent.getString("rawContent"));
				dbService.savePage(fetchedPageJsonContent.getInteger("id"),"Yo!", savePageHandler -> {
					dbService.fetchAllPages(context.asyncAssertSuccess(fetchAllPagesHandler -> {
						context.assertEquals(1, fetchAllPagesHandler.size());
						dbService.fetchPage("Test", context.asyncAssertSuccess(fetchedPageJsonContentUpdated -> {
							context.assertEquals("Yo!", fetchedPageJsonContentUpdated.getString("rawContent"));
							
							dbService.deletePage(fetchedPageJsonContentUpdated.getInteger("id"), context.asyncAssertSuccess(deletedPageHandler -> {
								dbService.fetchAllPages(context.asyncAssertSuccess(fetchAllPagesAfterDeleteHandler -> {
									context.assertTrue(fetchAllPagesAfterDeleteHandler.isEmpty(), "no pages should be remaining");
									async.complete();
								}));
							}));
						}));
					}));
				});
			}));
		}));
		async.awaitSuccess(5000);
	}
	
	@After
	public void finish(TestContext context) {
		vertx.close(context.asyncAssertSuccess());
	}
}
