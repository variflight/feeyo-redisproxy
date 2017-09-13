package com.feeyo.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 反射公共类
 */
public class ReflectUtil {
	
	private static final Logger LOG = LoggerFactory.getLogger(ReflectUtil.class);

	/**
	 * 构造反射类
	 *
	 */
	public static ReflectUtil on(String name) throws ReflectiveOperationException {
		return on(forName(name));
	}

	/**
	 * 包含 classloader的包装方法
	 *
	 */
	public static ReflectUtil on(String name, ClassLoader classLoader) throws ReflectiveOperationException {
		return on(forName(name, classLoader));
	}

	/**
	 * 包装方法
	 *
	 */
	public static ReflectUtil on(Class<?> clazz) {
		return new ReflectUtil(clazz);
	}

	/**
	 * 参数是一个Object对象
	 *
	 */
	public static ReflectUtil on(Object object) {
		return new ReflectUtil(object);
	}

	/**
	 * 放回一个可以访问的字段或者方法
	 *
	 */
	public static <T extends AccessibleObject> T setAndGetAccessible(T accessible) {
		if (accessible == null) {
			return null;
		}

		if (accessible instanceof Member) {
			Member member = (Member) accessible;

			if (Modifier.isPublic(member.getModifiers())
					&& Modifier.isPublic(member.getDeclaringClass().getModifiers())) {

				return accessible;
			}
		}

		if (!accessible.isAccessible()) {
			accessible.setAccessible(true);
		}

		return accessible;
	}

	private final Object object;

	private final boolean isClass;

	/**
	 * 构造函数
	 *
	 */
	private ReflectUtil(Class<?> type) {
		this.object = type;
		this.isClass = true;
	}

	/**
	 * 构造函数
	 *
	 */
	private ReflectUtil(Object object) {
		this.object = object;
		this.isClass = false;
	}

	/**
	 * 获取对象值
	 *
	 */
	@SuppressWarnings("unchecked")
	public <T> T get() {
		return (T) object;
	}

	/**
	 * 设置对象值
	 *
	 */
	public ReflectUtil setValue(String name, Object value) throws ReflectiveOperationException {
		Field field = field0(name);
		field.set(object, unwrap(value));
		return this;
	}

	/**
	 * 获取字段值
	 *
	 */
	public <T> T getValue(String name) throws ReflectiveOperationException {
		return getField(name).<T>get();
	}

	/**
	 * 获取某一个字段值
	 *
	 */
	public ReflectUtil getField(String name) throws ReflectiveOperationException {
		Field field = field0(name);
		return on(field.get(object));
	}

	private Field field0(String name) throws ReflectiveOperationException {
		Class<?> type = type();

		try {
			return type.getField(name);
		} catch (NoSuchFieldException e) {
			do {
				try {
					return setAndGetAccessible(type.getDeclaredField(name));
				} catch (NoSuchFieldException ignore) {
					LOG.debug("Ignore a NoSuchFieldException");
				}

				type = type.getSuperclass();
			} while (type != null);
			throw e;
		}
	}

	/**
	 * 获取一个类所有字段
	 *
	 */
	public Map<String, ReflectUtil> getFields() throws ReflectiveOperationException {
		Map<String, ReflectUtil> result = new LinkedHashMap<String, ReflectUtil>();
		Class<?> type = type();

		do {
			for (Field field : type.getDeclaredFields()) {
				if (!isClass ^ Modifier.isStatic(field.getModifiers())) {
					String name = field.getName();

					if (!result.containsKey(name))
						result.put(name, getField(name));
				}
			}

			type = type.getSuperclass();
		} while (type != null);

		return result;
	}

	/**
	 * 调用一个无参方法
	 *
	 */
	public ReflectUtil invoke(String name) throws ReflectiveOperationException {
		return invoke(name, new Object[0]);
	}

	/**
	 * 调用一个包含参数的方法
	 *
	 */
	public ReflectUtil invoke(String name, Object... args) throws ReflectiveOperationException {
		Class<?>[] types = types(args);

		try {
			Method method = invokeMethod(name, types);
			return on(method, object, args);
		} catch (NoSuchMethodException e) {
			try {
				Method method = getSimilarMethod(name, types);
				return on(method, object, args);
			} catch (NoSuchMethodException e1) {
				throw e1;
			}
		}
	}

	private Method invokeMethod(String name, Class<?>[] types) throws ReflectiveOperationException {
		Class<?> type = type();

		try {
			return type.getMethod(name, types);
		} catch (NoSuchMethodException e) {
			do {
				try {
					return type.getDeclaredMethod(name, types);
				} catch (NoSuchMethodException ignore) {
					LOG.debug("Ignore a NoSuchMethodException");
				}

				type = type.getSuperclass();
			} while (type != null);
			throw new NoSuchMethodException();
		}
	}

	private Method getSimilarMethod(String name, Class<?>[] types) throws ReflectiveOperationException {
		Class<?> type = type();
		for (Method method : type.getMethods()) {
			if (isSimilarSignature(method, name, types)) {
				return method;
			}
		}

		do {
			for (Method method : type.getDeclaredMethods()) {
				if (isSimilarSignature(method, name, types)) {
					return method;
				}
			}

			type = type.getSuperclass();
		} while (type != null);
		throw new ReflectiveOperationException("No similar method " + name + " with params " + Arrays.toString(types)
				+ " could be found on type " + type() + ".");
	}

	private boolean isSimilarSignature(Method possiblyMatchingMethod, String desiredMethodName,
			Class<?>[] desiredParamTypes) {
		return possiblyMatchingMethod.getName().equals(desiredMethodName)
				&& isMatch(possiblyMatchingMethod.getParameterTypes(), desiredParamTypes);
	}

	/**
	 * 创建一个对象实例
	 *
	 */
	public ReflectUtil newInstance() throws ReflectiveOperationException {
		return newInstance(new Object[0]);
	}

	/**
	 * 创建一个包含参数的对象实例
	 *
	 */
	public ReflectUtil newInstance(Object... args) throws ReflectiveOperationException {
		Class<?>[] types = types(args);

		try {
			Constructor<?> constructor = type().getDeclaredConstructor(types);
			return on(constructor, args);
		} catch (NoSuchMethodException e) {
			for (Constructor<?> constructor : type().getDeclaredConstructors()) {
				if (isMatch(constructor.getParameterTypes(), types)) {
					return on(constructor, args);
				}
			}

			throw e;
		}
	}

	private boolean isMatch(Class<?>[] declaredTypes, Class<?>[] actualTypes) {
		if (declaredTypes.length == actualTypes.length) {
			for (int i = 0; i < actualTypes.length; i++) {
				if (actualTypes[i] == NULL.class) {
					continue;
				}

				Class<?> wrapperClass = wrapper(declaredTypes[i]);
				if (wrapperClass == null) {
					continue;
				}

				if (wrapperClass.isAssignableFrom(wrapper(actualTypes[i]))) {
					continue;
				}

				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		return object.hashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ReflectUtil) {
			return object.equals(((ReflectUtil) obj).get());
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return object.toString();
	}

	private static ReflectUtil on(Constructor<?> constructor, Object... args) throws ReflectiveOperationException {
		Constructor<?> newConstructor = setAndGetAccessible(constructor);
		if (newConstructor == null) {
			throw new ReflectiveOperationException("Failed to execute setAndGetAccessible.");
		}
		return on(newConstructor.newInstance(args));

	}

	private static ReflectUtil on(Method method, Object object, Object... args) throws ReflectiveOperationException {
		setAndGetAccessible(method);

		if (method.getReturnType() == void.class) {
			method.invoke(object, args);
			return on(object);
		} else {
			return on(method.invoke(object, args));
		}
	}

	private static Object unwrap(Object object) {
		if (object instanceof ReflectUtil) {
			return ((ReflectUtil) object).get();
		}

		return object;
	}

	private static Class<?>[] types(Object... values) {
		if (values == null) {
			return new Class[0];
		}

		Class<?>[] result = new Class[values.length];

		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			result[i] = value == null ? NULL.class : value.getClass();
		}

		return result;
	}

	private static Class<?> forName(String name) throws ReflectiveOperationException {
		return Class.forName(name);
	}

	private static Class<?> forName(String name, ClassLoader classLoader) throws ReflectiveOperationException {
		return Class.forName(name, true, classLoader);
	}

	public Class<?> type() {
		if (isClass) {
			return (Class<?>) object;
		} else {
			return object.getClass();
		}
	}

	public static Class<?> wrapper(Class<?> type) {
		if (type == null) {
			return null;
		}
		if (!type.isPrimitive()) {
			return type;
		}

		return getPrimitiveType(type);
	}

	private static Class<?> getPrimitiveType(Class<?> type) {
		Class<?> x = getNumberType(type);
		if (x != null)
			return x;

		if (boolean.class == type) {
			return Boolean.class;
		}

		Class<?> x1 = getStringType(type);
		if (x1 != null)
			return x1;

		return type;
	}

	private static Class<?> getStringType(Class<?> type) {
		if (byte.class == type) {
			return Byte.class;
		}

		if (char.class == type) {
			return Character.class;
		}
		return null;
	}

	private static Class<?> getNumberType(Class<?> type) {
		if (short.class == type) {
			return Short.class;
		}

		if (int.class == type) {
			return Integer.class;
		}

		if (long.class == type) {
			return Long.class;
		}

		if (double.class == type) {
			return Double.class;
		}

		if (float.class == type) {
			return Float.class;
		}
		return null;
	}

	private static class NULL {
	}
}
