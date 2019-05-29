package com.feeyo.util.topn;

import java.util.List;

public class TopNTest {

	public static void main(String[] args) {

		TopNCounter<String> vs = new TopNCounter<String>(3);
		String[] stream = { "X", "X", "Y", "Z", "A", "B", "C", "X", "X", "A", "A", "A", "Y" };
		for (String i : stream) {
			vs.offer(i);
		}

		//
		vs.retain(5);
		
		List<Counter<String>> topk = vs.topK(6);

		for (Counter<String> top : topk) {
			System.out.println(top.getItem() + ":" + top.getCount());
		}
	}

}
