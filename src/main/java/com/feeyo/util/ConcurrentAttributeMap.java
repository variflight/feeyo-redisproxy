package com.feeyo.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class ConcurrentAttributeMap implements AttributeMap {
	
	private final AttributeNamespace namespace;
	private volatile Object[] values;
	private volatile ConcurrentHashMap<Integer, Object> backupMap;
	
	private static final Unsafe unsafe;
	private static final long ABASE;
	private static final int ASHIFT;

	public ConcurrentAttributeMap(AttributeNamespace namespace, int size) {
		this.namespace = namespace;
		this.values = new Object[size];
	}

	public synchronized Object put(AttributeKey key, Object value) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return getBackupMap().put(Integer.valueOf(id), value);
		}
		Object old = getObjectVolatile(id);
		putObjectVolatile(id, value);
		return old;
	}

	public Object get(AttributeKey key) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return getBackupMap().get(Integer.valueOf(id));
		}
		return getObjectVolatile(id);
	}

	public synchronized Object putIfAbsent(AttributeKey key, Object value) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return getBackupMap().putIfAbsent(Integer.valueOf(id), value);
		}
		Object old = getObjectVolatile(id);
		if (old == null) {
			putObjectVolatile(id, value);
			return null;
		}
		return old;
	}

	public synchronized Object remove(AttributeKey key) {
		Object[] elements = this.values;
		int id = key.getId();
		if (id >= elements.length) {
			return getBackupMap().remove(Integer.valueOf(id));
		}
		Object old = getObjectVolatile(id);
		putObjectVolatile(id, null);
		return old;
	}

	public Map<Object, Object> toMap() {
		Map<Object, Object> map = new HashMap<Object, Object>();
		Object[] elements = this.values;
		for (int i = 0; i < elements.length; i++) {
			Object o = getObjectVolatile(i);
			if (o != null) {
				AttributeKey key = this.namespace.get(i);
				if (key != null) {
					map.put(key.getName(), o);
				}
			}
		}
		for (Map.Entry<Integer, Object> entry : getBackupMap().entrySet()) {
			AttributeKey key = this.namespace.get(((Integer) entry.getKey()).intValue());
			if (key != null) {
				map.put(key.getName(), entry.getValue());
			}
		}
		return map;
	}

	private ConcurrentHashMap<Integer, Object> getBackupMap() {
		if (this.backupMap == null) {
			synchronized (this) {
				if (this.backupMap == null) {
					this.backupMap = new ConcurrentHashMap<Integer, Object>();
				}
			}
		}
		return this.backupMap;
	}

	private final Object getObjectVolatile(int i) {
		return unsafe.getObjectVolatile(this.values, (i << ASHIFT) + ABASE);
	}

	@SuppressWarnings("unused")
	private final boolean compareAndSwapObject(int i, Object expected, Object value) {
		return unsafe.compareAndSwapObject(this.values, (i << ASHIFT) + ABASE, expected, value);
	}

	private final void putObjectVolatile(int i, Object value) {
		unsafe.putObjectVolatile(this.values, (i << ASHIFT) + ABASE, value);
	}

	static {
		try {
			Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
			Field theUnsafe = null;
			for (Field field : unsafeClass.getDeclaredFields()) {
				if (field.getName().equals("theUnsafe")) {
					theUnsafe = field;
				}
			}
			
			if (theUnsafe != null) {
				theUnsafe.setAccessible(true);
				unsafe = (Unsafe) theUnsafe.get(null);
			} else {
				unsafe = null;
			}
			
			ABASE = unsafe.arrayBaseOffset(Object[].class);
			int scale = unsafe.arrayIndexScale(Object[].class);
			ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
}
