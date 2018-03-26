package com.feeyo.util.mpmc;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;


@SuppressWarnings("restriction")
public class UnsafeUtil {
	
	public static final Unsafe UNSAFE;

	static {
		try {
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
				public Unsafe run() throws Exception {
					final Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
					theUnsafe.setAccessible(true);
					return (Unsafe) theUnsafe.get(null);
				}
			};

			UNSAFE = AccessController.doPrivileged(action);
		} catch (final Exception ex) {
			throw new RuntimeException("Unable to load unsafe", ex);
		}
	}

}
