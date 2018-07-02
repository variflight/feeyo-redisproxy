package com.feeyo.util.gc;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.util.RandomUtil;

public class ProactiveGcTaskDemo {
	
	public static void main(String[] args) throws IOException {
		//// 真实代码示例 ////
		CleanUpScheduler scheduler = new CleanUpScheduler();
		ProactiveGcTask task = new ProactiveGcTask(scheduler, 50);
		scheduler.schedule("03:30-04:30", task);
		// ....
		scheduler.shutdown();

		///// 演示用代码 ////
		// 模拟内存占用, 根据运行环境调整到可以占用一半以上老生代
		final Enchanter enchanter = new Enchanter();
		enchanter.makeGarbage("10000000");

		// 直接运行看效果
		task.run();

		System.out.println("hit ENTER to stop");
		System.in.read();
		enchanter.clearGarbage();
		////////////////

	}
	
	static class Enchanter {
		private static final Logger log = LoggerFactory.getLogger(Enchanter.class);
		private static final String[] ha = { "苟利国家生死以%d岂因祸福趋避之", "煮豆燃豆萁豆在釜中泣%d本是同根生相煎何太急", "利欲驱人万火牛%d江湖浪迹一沙鸥",
				"且持梦笔书奇景%d日破云涛万里红", "春来我不先开口%d哪个虫儿敢作声" };

		private List<String> garbage = new ArrayList<>();

		public void makeGarbage(String val) {
			log.info("trying to occupy oldGen");
			int size = Integer.parseInt(val);
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < size; i++) {
				// randomize a little bit to avoid duplicate strings
				sb.append(String.format(ha[RandomUtil.nextInt(i % ha.length + 1)], i));
			}
			// 大对象直接进old gen，用list hold住不释放
			garbage.add(sb.toString());
			log.info("Enchanter is littering around, garbage size: {}", sb.length());
		}

		public void clearGarbage() {
			// 清空list以便cms可以回收
			garbage.clear();
			log.info("garbage cleared from list...");
		}

	}
}
