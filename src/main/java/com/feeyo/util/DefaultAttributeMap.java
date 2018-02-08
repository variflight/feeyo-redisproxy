package com.feeyo.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DefaultAttributeMap implements AttributeMap {
	
	private final AttributeNamespace namespace;
	private volatile Object[] values;

	public DefaultAttributeMap(AttributeNamespace namespace, int size) {
		this.namespace = namespace;
		this.values = new Object[size];
	}

	public Object put(AttributeKey key, Object value) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			elements = Arrays.copyOf(elements, id + 1);
			this.values = elements;
		}
		Object old = elements[id];
		elements[id] = value;
		return old;
	}

	public Object get(AttributeKey key) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return null;
		}
		return elements[id];
	}

	public Object putIfAbsent(AttributeKey key, Object value) {
		throw new UnsupportedOperationException();
	}

	public Object remove(AttributeKey key) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return null;
		}
		Object old = elements[id];
		elements[id] = null;
		return old;
	}

	public Map<Object, Object> toMap() {
		Map<Object, Object> map = new HashMap<Object, Object>();
		Object[] elements = this.values;
		for (int i = 0; i < elements.length; i++) {
			if (elements[i] != null) {
				AttributeKey key = this.namespace.get(i);
				if (key != null) {
					map.put(key.getName(), elements[i]);
				}
			}
		}
		return map;
	}
}
