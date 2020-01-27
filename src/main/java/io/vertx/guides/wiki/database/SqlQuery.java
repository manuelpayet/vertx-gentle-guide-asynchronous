package io.vertx.guides.wiki.database;

enum SqlQuery {

	CREATE_PAGES_TABLE("create-pages-table"), ALL_PAGES("all-pages"), ALL_PAGES_DATA("all-pages-data"), GET_PAGE("get-page"),
	CREATE_PAGE("create-page"), SAVE_PAGE("save-page"), DELETE_PAGE("delete-page");

	private final String key;

	private SqlQuery(final String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}

}