package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.AuthInitializerVerticle;

public class MainVerticle extends AbstractVerticle {
	@Override
	public void start(Promise<Void> promise) throws Exception {
		final Promise<String> dbVerticleDeployment = Promise.promise();
		vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment);
		
		Future<String> authDeployedFuture = dbVerticleDeployment.future().compose(dbDeployedHandler -> {
			final Promise<String> promiseAuthDeployed = Promise.promise();
			vertx.deployVerticle(new AuthInitializerVerticle(), promiseAuthDeployed);
			return promiseAuthDeployed.future();
		});
		
		authDeployedFuture.compose(httpFuture -> {
			final Promise<String> httpServerPromise = Promise.promise();
			
			vertx.deployVerticle("io.vertx.guides.wiki.http.HttpServerVerticle",
									new DeploymentOptions().setInstances(2), 
								httpServerPromise);
			
			return httpServerPromise.future();
		}).setHandler(allVerticlesInitiated -> {
			if(allVerticlesInitiated.succeeded()) {
				promise.complete();
			} else {
				promise.fail(allVerticlesInitiated.cause());
			}
		});
		
	}
}
