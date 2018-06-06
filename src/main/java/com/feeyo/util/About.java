package com.feeyo.util;

public class About {
	
	public static String help(String path ) {
		
		StringBuffer buffer = new StringBuffer();
		
		buffer.append("                   _ooOoo_").append("\n");
		buffer.append("                  o8888888o").append("\n");
		buffer.append("                  88\" . \"88").append("\n");
		buffer.append("                  (| -_- |)").append("\n");
		buffer.append("                  O\\  =  /O").append("\n");
		buffer.append("               ____/`---'\\____").append("\n");
		buffer.append("             .'  \\|     |//  `.").append("\n");
		buffer.append("            /  \\\\|||  :  |||//  \\").append("\n");
		buffer.append("           /  _||||| -:- |||||-  \\").append("\n");
		buffer.append("           |   | \\\\\\  -  /// |   |").append("\n");
		buffer.append("           | \\_|  ''\\---/''  |   |").append("\n");
		buffer.append("           \\  .-\\__  `-`  ___/-. /").append("\n");
		buffer.append("         ___`. .'  /--.--\\  `. . __").append("\n");
		buffer.append("      .\"\" '<  `.___\\_<|>_/___.'  >'\"\".").append("\n");
		buffer.append("     | | :  `- \\`.;`\\ _ /`;.`/ - ` : | |").append("\n");
		buffer.append("     \\  \\ `-.   \\_ __\\ /__ _/   .-` /  /").append("\n");
		buffer.append("======`-.____`-.___\\_____/___.-`____.-'======").append("\n");
		buffer.append("").append("\n");
		buffer.append("============== 佛祖保佑  永无BUG ==============").append("\n");
		buffer.append("----          Redis & Kafka Proxy          ").append("\n");
		buffer.append("----         path=").append( path ).append("\n");
		buffer.append("----         startup=").append( System.currentTimeMillis() ).append("\n");
		buffer.append("=============================================\n");
		
		return buffer.toString(); 
	}
}
