package com.feeyo.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AttributeNamespace {
	
	private static final ConcurrentHashMap<String, AttributeNamespace> name2Instance 
								= new ConcurrentHashMap<String, AttributeNamespace>();
	private final String namespace;

	public static AttributeNamespace getOrCreateNamespace(String namespace) {
		AttributeNamespace instance = (AttributeNamespace) name2Instance.get(namespace);
		if (instance == null) {
			instance = new AttributeNamespace(namespace);
			AttributeNamespace old = (AttributeNamespace) name2Instance.putIfAbsent(namespace, instance);
			if (old != null) {
				instance = old;
			}
		}
		return instance;
	}

	public static AttributeNamespace createNamespace(String namespace) {
		AttributeNamespace key = new AttributeNamespace(namespace);
		AttributeNamespace old = (AttributeNamespace) name2Instance.putIfAbsent(namespace, key);
		if (old != null) {
			throw new IllegalArgumentException(
					String.format("namespace '%s' is already in use", new Object[] { namespace }));
		}
		return key;
	}

	private final AtomicInteger idGen = new AtomicInteger();
	private final ConcurrentHashMap<String, AttributeKey> name2AttributeKey = new ConcurrentHashMap<String, AttributeKey>();
	private final ConcurrentHashMap<Integer, AttributeKey> id2AttributeKey = new ConcurrentHashMap<Integer, AttributeKey>();

	private AttributeNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getNamespace() {
		return this.namespace;
	}

	public AtomicInteger getIdGen() {
		return this.idGen;
	}

	public AttributeKey getOrCreate(String name) {
		AttributeKey key = (AttributeKey) this.name2AttributeKey.get(name);
		if (key == null) {
			key = new AttributeKey(this, name);
			AttributeKey old = (AttributeKey) this.name2AttributeKey.putIfAbsent(name, key);
			if (old == null) {
				cacheForId(key);
			} else {
				key = old;
			}
		}
		return key;
	}

	public AttributeKey create(String name) {
		AttributeKey key = new AttributeKey(this, name);
		AttributeKey old = (AttributeKey) this.name2AttributeKey.putIfAbsent(name, key);
		if (old != null) {
			throw new IllegalArgumentException(String.format("name '%s' in namespace '%s' is already in use",
					new Object[] { name, this.namespace }));
		}
		cacheForId(key);
		return key;
	}

	private void cacheForId(AttributeKey key) {
		this.id2AttributeKey.put(Integer.valueOf(key.getId()), key);
	}

	public AttributeKey get(int id) {
		return (AttributeKey) this.id2AttributeKey.get(Integer.valueOf(id));
	}
}
