package io.vertx.guides.wiki;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.guides.wiki.database.WikiDatabaseVerticle;
import io.vertx.guides.wiki.http.AuthInitializerVerticle;
import io.vertx.reactivex.core.AbstractVerticle;

public class MainVerticle extends AbstractVerticle {
	@Override
	public void start(Promise<Void> promise) throws Exception {
		
		vertx.rxDeployVerticle(new WikiDatabaseVerticle())
		.flatMap(id -> {
			return vertx.rxDeployVerticle(new AuthInitializerVerticle());
		}).flatMap(httpFuture -> {
			return vertx.rxDeployVerticle("io.vertx.guides.wiki.http.HttpServerVerticle",
									new DeploymentOptions().setInstances(2));
			
		}).subscribe(id -> promise.complete(), promise::fail);
		
	}
}
