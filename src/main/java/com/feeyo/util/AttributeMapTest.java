package com.feeyo.util;

import java.util.HashMap;

public class AttributeMapTest {
	
	private static final AttributeNamespace NS = AttributeNamespace.createNamespace("mynamespace");
	private static DefaultAttributeMap attributeMap = new DefaultAttributeMap(NS, 8);
	private static ConcurrentAttributeMap attributeMap2 = new ConcurrentAttributeMap(NS, 8);
	
	private static HashMap<String, String> map1 = new HashMap<String, String>();
	
	public static void main(String[] args) {
		
		int length = 220000;
		
	
		AttributeKey[] keys = new AttributeKey[length];
		
		for (int i = 0; i<length; i++) {
			keys[i] = new AttributeKey(NS, "mykey_" + i);
			
			attributeMap.put(keys[i], "aa_" +i);
			attributeMap2.put(keys[i], "aa_" +i);
		}
		
		//
		//------------------------11111-------------------------------
		long t1 = System.nanoTime();
		Object value = attributeMap.get( keys[length-1] );
		long t2 = System.nanoTime();
		
		System.out.println( "##11#### "+ value + " diff="+(t2-t1)  );
		
		
		//
		//------------------------22222-------------------------------
		long t3 = System.nanoTime();
		Object value1 = attributeMap.get( keys[length-2] );
		long t4 = System.nanoTime();
		
		System.out.println( "##22#### "+ value1 + " diff="+(t4-t3)  );
		

		//
		//------------------------33333---------------------------------
		for (int i = 0; i<length; i++) {
			map1.put("mykey_"+ i,  "aa_" +i);
		}
		
		String k = "mykey_"+(length-1);
		
		long t5 = System.nanoTime();
		Object value2 = map1.get( k );
		long t6 = System.nanoTime();
		System.out.println("##33#### "+ value2 + " diff="+(t6-t5)  );
		
		
		
		
		
		
	}

}
