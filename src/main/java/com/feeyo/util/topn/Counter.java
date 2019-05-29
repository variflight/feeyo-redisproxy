package com.feeyo.util.topn;

import java.io.Serializable;

/**
 * Modified from Counter.java in https://github.com/addthis/stream-lib
 */
public class Counter<T> implements Serializable{

	private static final long serialVersionUID = -397774909139321516L;
	
	protected T item;
    protected double count;

    /**
     * For de-serialization
     */
    public Counter() {
    }

    public Counter(T item) {
        this.count = 0;
        this.item = item;
    }

    public Counter(T item, double count) {
        this.item = item;
        this.count = count;
    }


    public T getItem() {
        return item;
    }

    public double getCount() {
        return count;
    }

    public void setCount(double count) {
        this.count = count;
    }
    @Override
    public String toString() {
        return item + ":" + count;
    }

}
