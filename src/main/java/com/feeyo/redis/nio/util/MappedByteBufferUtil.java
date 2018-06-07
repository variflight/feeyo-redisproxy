package com.feeyo.redis.nio.util;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MappedByteBufferUtil {
	
	private static Logger LOGGER = LoggerFactory.getLogger(MappedByteBufferUtil.class);

	public static void clean(final MappedByteBuffer buffer) {
		
		AccessController.doPrivileged(new PrivilegedAction<MappedByteBuffer>() {
			@SuppressWarnings("restriction")
			public MappedByteBuffer run() {
				try {
					Method cleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
					cleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) cleanerMethod.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					LOGGER.error("Cannot clean mapped", e);
				}
				return null;
			}
		});

	}
}
