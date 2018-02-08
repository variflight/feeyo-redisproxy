package com.feeyo.util;

public class AttributeKey {
	
	private final AttributeNamespace namespace;
	private final String name;
	private final int id;

	protected AttributeKey(AttributeNamespace namespace, String name) {
		this.namespace = namespace;
		this.name = name;
		this.id = namespace.getIdGen().getAndIncrement();
	}

	public int getId() {
		return this.id;
	}

	public String getName() {
		return this.name;
	}

	public AttributeNamespace getNamespace() {
		return this.namespace;
	}
}
