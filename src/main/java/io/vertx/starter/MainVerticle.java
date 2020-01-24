package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;

public class MainVerticle extends AbstractVerticle {
	@Override
	public void start(Promise<Void> promise) throws Exception {
		final Promise<String> dbVerticleDeployment = Promise.promise();
		vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment);
		
		dbVerticleDeployment.future().compose(httpFuture -> {
			final Promise<String> httpServerPromise = Promise.promise();
			
			vertx.deployVerticle("io.vertx.starter.HttpServerVerticle",
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
