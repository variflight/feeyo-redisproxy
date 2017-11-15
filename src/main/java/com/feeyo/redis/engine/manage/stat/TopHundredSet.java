package com.feeyo.redis.engine.manage.stat;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class TopHundredSet{

	private final int TOP_LIST_LENGTH = 100;
	
	class KeyUnitComparator implements Comparator<KeyUnit> {
		@Override
		public int compare(KeyUnit keyUnit1, KeyUnit keyUnit2) {
			if( keyUnit1.getLength().get() > keyUnit2.getLength().get() )
				return -1 ;
			if (keyUnit1.getLength().get() == keyUnit2.getLength().get())
				return 0;
			return 1;
		}
		
	}
	
	private Set<KeyUnit> set = Collections.synchronizedSet(new TreeSet<KeyUnit>(new KeyUnitComparator()));
	
	private synchronized int size(){
		return set.size();
	}
	
	public synchronized boolean contains(String key){
		if(null != key) {
			for(KeyUnit keyUnit : set) {
				if(key.equals(keyUnit.getKey()))
					return true;
			}
		}
		return false;
	}
	
	private synchronized boolean remove(String key){
		if(null != key) {
			for(KeyUnit keyUnit : set) {
				if(key.equals(keyUnit.getKey()))
					return set.remove(keyUnit);
			}
		}
		return false;
	}
	
	public synchronized KeyUnit find(String key){
		for(KeyUnit KeyUnit : set){
			if(KeyUnit.getKey().equals(key))
			return KeyUnit ;
		}
		return null;
	}
	
	public synchronized boolean add(KeyUnit keyUnit){
		if(null == keyUnit)
			return false;
		if(contains(keyUnit.getKey())){
			remove(keyUnit.getKey());
		}else if(isFull()){
			KeyUnit minKeyUnit = findMinLengthUnit();
			if(minKeyUnit.getLength().get() <= keyUnit.getLength().get())
				return false;
			else{
				remove(minKeyUnit.getKey());
				return set.add(keyUnit);
			}
		}
		return set.add(keyUnit);
	}
	
	public synchronized boolean isFull(){
		return size() >= TOP_LIST_LENGTH;
	}
	
	private KeyUnit findMinLengthUnit(){
		long length = 0;
		KeyUnit minKeyUnit = null;
		for(KeyUnit KeyUnit : set){
			long keyLength = KeyUnit.getLength().get();
			if(keyLength<length){
				length = keyLength;
				minKeyUnit = KeyUnit ;
			}
		}
		return minKeyUnit ;
	}
	
	public Set<KeyUnit> getKeyUnits(){
		return set;
	}
}
