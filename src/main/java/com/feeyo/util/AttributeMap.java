package com.feeyo.util;

public interface AttributeMap {
	
	public Object put(AttributeKey key, Object value);

	public Object get(AttributeKey key);

	public Object putIfAbsent(AttributeKey key, Object value);

	public Object remove(AttributeKey key);
}
