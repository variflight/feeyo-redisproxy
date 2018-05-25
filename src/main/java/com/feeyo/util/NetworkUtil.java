package com.feeyo.util;

import java.io.IOException;
import java.net.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.redis.engine.RedisEngineCtx;

public class NetworkUtil {
	
	private static Logger LOGGER = LoggerFactory.getLogger( NetworkUtil.class );

	public static String getLocalIp() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			ArrayList<String> ipv4Result = new ArrayList<String>();
			ArrayList<String> ipv6Result = new ArrayList<String>();
			while (enumeration.hasMoreElements()) {
				final NetworkInterface networkInterface = enumeration.nextElement();
				final Enumeration<InetAddress> en = networkInterface.getInetAddresses();
				while (en.hasMoreElements()) {
					final InetAddress address = en.nextElement();
					if (!address.isLoopbackAddress()) {
						if (address instanceof Inet6Address) {
							ipv6Result.add(normalizeHostAddress(address));
						} else {
							ipv4Result.add(normalizeHostAddress(address));
						}
					}
				}
			}

			// 优先返回 ipv4
			if (!ipv4Result.isEmpty()) {
				for (String ip : ipv4Result) {
					if (ip.startsWith("127.0") || ip.startsWith("192.168")) {
						continue;
					}

					return ip;
				}

				return ipv4Result.get(ipv4Result.size() - 1);
			} else if (!ipv6Result.isEmpty()) {
				return ipv6Result.get(0);
			}
			
			// 如果没有找到，返回 localhost
			final InetAddress localHost = InetAddress.getLocalHost();
			return normalizeHostAddress(localHost);
		} catch (Exception e) {
			LOGGER.error("Failed to obtain local address", e);
		}

		return null;
	}

	public static String normalizeHostAddress(final InetAddress localHost) {
		if (localHost instanceof Inet6Address) {
			return "[" + localHost.getHostAddress() + "]";
		} else {
			return localHost.getHostAddress();
		}
	}
	
	public static SocketAddress string2SocketAddress(final String addr) {
		String[] s = addr.split(":");
		InetSocketAddress isa = new InetSocketAddress(s[0], Integer.parseInt(s[1]));
		return isa;
	}

	public static String socketAddress2String(final SocketAddress addr) {
		StringBuilder sb = new StringBuilder();
		InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
		sb.append(inetSocketAddress.getAddress().getHostAddress());
		sb.append(":");
		sb.append(inetSocketAddress.getPort());
		return sb.toString();
	}

	public static String getDefaultRouteIf() {
		if (!JavaUtils.isLinux())
			return null;

		String gateWay = null;
		try {
			gateWay = ShellUtils.execCommand("bash", "-c", "route -n |grep '^0.0.0.0' | awk  'BEGIN{ORS=\"\"} NR==1 {print $8}'");
			if (gateWay.equals(""))
				return null;
		} catch (IOException e) {
			gateWay = null;
		}

		return gateWay;
	}

	public static String getDefaultRouteIp() {
		String ip = null;
		try {
			if (JavaUtils.isWindows()) {
				String ret = ShellUtils.execCommand("cmd.exe", "/C", "route print -4 | find \" 0.0.0.0\" ");
				String[] ips = ret.split("\\s+");
				if (ips.length > 2)
					ip = ips[ips.length-2];
			} else {
				ip = ShellUtils.execCommand("bash", "-c", "IF=`route -n | grep '^0.0.0.0' | awk 'NR==1 {print $8}'` && ifconfig $IF | grep -P \"\\d+(\\.\\d+){3}\" -o | head -n1 | tr -d '\\n'" );
			}

			if (ip == null || ip.equals(""))
				return null;
		} catch (IOException e) {
			LOGGER.error("Failed to obtain default route ip", e);
			ip = null;
		}

		return ip;
	}

	public static String getIp() {
		try {
			Enumeration<NetworkInterface> enumeration = NetworkInterface.getNetworkInterfaces();
			final String defaultRouteIp = getDefaultRouteIp();
			if (defaultRouteIp != null)
				return defaultRouteIp;

			final String defaultRouteDevice = getDefaultRouteIf();

			List<NetworkInterface> list= Collections.list(enumeration);

			Collections.sort(list, Collections.reverseOrder(new Comparator<NetworkInterface>() {
				@Override
				public int compare(NetworkInterface nif1, NetworkInterface nif2) {
					try {
						if (nif1.getName().equals(defaultRouteDevice))
							return 1;

						if (nif2.getName().equals(defaultRouteDevice))
							return -1;

						if (nif1.isLoopback())
							return -1;

						if (nif2.isLoopback())
							return 1;

						return 0;

					} catch (SocketException e) {
						LOGGER.error("Failed to obtain interface name", e);
					}
					return 0;
				}
			}));

			for (NetworkInterface networkInterface : list) {
				List<InterfaceAddress> ifas = networkInterface.getInterfaceAddresses();
				for (InterfaceAddress ifa : ifas) {
					InetAddress address = ifa.getAddress();
					if (address instanceof Inet4Address) {
						return address.getHostAddress();
					}
				}
			}

		} catch (SocketException e) {
			LOGGER.error("Failed to get ip", e);
		}

		return null;
	}

	public static String getLocalAddress() {
		String ip = getLocalIp();
		
		Map<String, String> map = RedisEngineCtx.INSTANCE().getServerMap();
		String port = map.get("port");
		
		StringBuffer sb = new StringBuffer();
		sb.append(ip).append(":").append(port);
		return sb.toString();
	}
}
