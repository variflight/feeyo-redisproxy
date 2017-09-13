package com.feeyo.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(JavaUtils.class);
	
	private static String OS = System.getProperty("os.name").toLowerCase();
	
	public static boolean isLinux() {
		return OS.contains("linux");
	}

	public static boolean isMacOS() {
		return OS.contains("mac") && OS.indexOf("os") > 0 && !OS.contains("x");
	}

	public static boolean isMacOSX() {
		return OS.contains("mac") && OS.indexOf("os") > 0 && OS.indexOf("x") > 0;
	}

	public static boolean isMac() {
		return OS.contains("mac") && OS.indexOf("os") > 0;
	}

	public static boolean isWindows() {
		return OS.contains("windows");
	}
	
	
	/**
     * This function is only for linux
     */
    public static boolean isProcDead(String pid) {
        if (!isLinux()) {
            return false;
        }

        String path = "/proc/" + pid;
        File file = new File(path);

        if (!file.exists()) {
            LOGGER.info("Process " + pid + " is dead");
            return true;
        }
        return false;
    }
	
	/**
     * Gets the pid of current JVM, because Java doesn't provide a real way to do this.
     */
    public static String process_pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String[] split = name.split("@");
        if (split.length != 2) {
            throw new RuntimeException("Got unexpected process name: " + name);
        }

        return split[0];
    }
    
    
    public static void kill(Integer pid) {
        process_killed(pid);
        sleepMs(2 * 1000);
        ensure_process_killed(pid);
    }

    public static void kill_signal(Integer pid, String signal) {
        String cmd = "kill " + signal + " " + pid;
        try {
            exec_command(cmd);
            LOGGER.info(cmd);
        } catch (IOException e) {
        	LOGGER.info("Error when run " + cmd + ". Process has been killed. ");
        } catch (Exception e) {
        	LOGGER.info("Error when run " + cmd + ". Exception ", e);
        }
    }
    
    public static void ensure_process_killed(Integer pid) {
        // just kill the process 5 times
        // to make sure the process is killed ultimately
        for (int i = 0; i < 5; i++) {
            try {
                exec_command("kill -9 " + pid);
                LOGGER.info("kill -9 process " + pid);
                sleepMs(100);
            } catch (IOException e) {
            	LOGGER.info("Error when trying to kill " + pid + ". Process has been killed");
            } catch (Exception e) {
            	LOGGER.info("Error when trying to kill " + pid + ".Exception ", e);
            }
        }
    }

    public static void process_killed(Integer pid) {
        try {
            exec_command("kill " + pid);
            LOGGER.info("kill process " + pid);
        } catch (IOException e) {
        	LOGGER.info("Error when trying to kill " + pid + ". Process has been killed. ");
        } catch (Exception e) {
        	LOGGER.info("Error when trying to kill " + pid + ".Exception ", e);
        }
    }
    
    public static void haltProcess(int val) {
        Runtime.getRuntime().halt(val);
    }
    
    
    /**
     * use launchProcess to execute a command
     *
     * @param command command to be executed
     * @throws ExecuteException
     * @throws IOException
     */
    public static void exec_command(String command) throws IOException {
        launchProcess(command, new HashMap<String, String>(), false);
    }
    
	protected static java.lang.Process launchProcess(final List<String> cmdlist, final Map<String, String> environment)
			throws IOException {
		ProcessBuilder builder = new ProcessBuilder(cmdlist);
		builder.redirectErrorStream(true);
		Map<String, String> process_evn = builder.environment();
		for (Entry<String, String> entry : environment.entrySet()) {
			process_evn.put(entry.getKey(), entry.getValue());
		}

		return builder.start();
	}
	
	public static String launchProcess(final String command, final List<String> cmdlist,
			final Map<String, String> environment, boolean backend) throws IOException {
		if (backend) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					List<String> cmdWrapper = new ArrayList<>();

					cmdWrapper.add("nohup");
					cmdWrapper.addAll(cmdlist);
					cmdWrapper.add("&");

					try {
						launchProcess(cmdWrapper, environment);
					} catch (IOException e) {
						LOGGER.error("Failed to run nohup " + command + " &," + e.getCause(), e);
					}
				}
			}).start();
			return null;
		} else {
			try {
				Process process = launchProcess(cmdlist, environment);

				StringBuilder sb = new StringBuilder();
				String output = JavaUtils.getOutput(process.getInputStream());
				String errorOutput = JavaUtils.getOutput(process.getErrorStream());
				sb.append(output);
				sb.append("\n");
				sb.append(errorOutput);

				int ret = process.waitFor();
				if (ret != 0) {
					LOGGER.warn(command + " is terminated abnormally. ret={}, str={}", ret, sb.toString());
				}
				LOGGER.debug("command {}, ret {}, str={} :", new Object[]{ command, ret, sb.toString() } );
				return sb.toString();
			} catch (Throwable e) {
				LOGGER.error("Failed to run " + command + ", " + e.getCause(), e);
			}

			return "";
		}
	}

	/**
	 * it should use DefaultExecutor to start a process, but some little problem
	 * have been found, such as exitCode/output string so still use the old
	 * method to start process
	 *
	 * @param command      command to be executed
	 * @param environment  env vars
	 * @param backend	   whether the command is executed at backend
	 * @return outputString
	 * @throws IOException
	 */
	public static String launchProcess(final String command, final Map<String, String> environment, boolean backend)
			throws IOException {
		String[] cmds = command.split(" ");

		ArrayList<String> cmdList = new ArrayList<>();
		for (String tok : cmds) {
			if ( tok != null && !"".equals( tok )) {
				cmdList.add(tok);
			}
		}

		return launchProcess(command, cmdList, environment, backend);
	}
	
	public static String getOutput(InputStream input) {
        BufferedReader in = new BufferedReader(new InputStreamReader(input));
        StringBuilder sb = new StringBuilder();
        String line;
        try {
            while ((line = in.readLine()) != null) {
                sb.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return sb.toString();
    }

	///
	private static long TB = 1L << 40;
	private static long GB = 1L << 30;
	private static long MB = 1L << 20;
	private static long KB = 1L << 10;


	public static String bytesToString2(long size) {
	
		int value = 0;
		String unit = null;

		if (size >= 2 * TB) {
			value = (int) (size / TB);
			unit = "TB";
		} else if (size >= 2 * GB) {
			value = (int) (size / GB);
			unit = "GB";
		} else if (size >= 2 * MB) {
			value = (int) (size / MB);
			unit = "MB";
		} else if (size >= 2 * KB) {
			value = (int) (size / KB);
			unit = "KB";
		} else {
			value = (int) size;
			unit = "B";
		}
		
		return value + unit;
	}
	
	public static void sleepMs(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException ignored) {
		}
	}
	
	private static String HEXES = "0123456789ABCDEF";
	public static String toPrintableString(byte[] buf) {
        if (buf == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (byte b : buf) {
            if (index % 10 == 0) {
                sb.append("\n");
            }
            index++;

            sb.append(HEXES.charAt((b & 0xF0) >> 4));
            sb.append(HEXES.charAt((b & 0x0F)));
            sb.append(" ");
        }

        return sb.toString();
    }
	
	
	// 获取系统资源
	// #################################################
	public static Double getCpuUsage() {
		if (!isLinux()) {
			return 0.0;
		}

		Double value;
		String output = null;
		try {
			String pid = JavaUtils.process_pid();
			String command = String.format("top -b -n 1 -p %s | grep -w %s", pid, pid);
			output = SystemOperation.exec(command);
			String subStr = output.substring(output.indexOf("S") + 1);
			for (int i = 0; i < subStr.length(); i++) {
				char ch = subStr.charAt(i);
				if (ch != ' ') {
					subStr = subStr.substring(i);
					break;
				}
			}
			String usedCpu = subStr.substring(0, subStr.indexOf(" "));
			value = Double.valueOf(usedCpu);
		} catch (Exception e) {
			LOGGER.warn("Failed to get cpu usage ratio.");
			if (output != null)
				LOGGER.warn("Output string is \"" + output + "\"");
			value = 0.0;
		}

		return value;
	}
	
	
	public static Double getMemUsage() {
		if ( isLinux() ) {
			try {
				Double value;
				String pid = JavaUtils.process_pid();
				String command = String.format("top -b -n 1 -p %s | grep -w %s", pid, pid);
				String output = SystemOperation.exec(command);

				int m = 0;
				String[] strArray = output.split(" ");
				for (int i = 0; i < strArray.length; i++) {
					String info = strArray[i];
					if (info.trim().length() == 0) {
						continue;
					}
					if (m == 5) {
						// memory
						String unit = info.substring(info.length() - 1);

						if (unit.equalsIgnoreCase("g")) {
							value = Double.parseDouble(info.substring(0, info.length() - 1));
							value *= 1000000000;
						} else if (unit.equalsIgnoreCase("m")) {
							value = Double.parseDouble(info.substring(0, info.length() - 1));
							value *= 1000000;
						} else if (unit.equalsIgnoreCase("t")) {
							value = Double.parseDouble(info.substring(0, info.length() - 1));
							value *= 1000000000000l;
						} else {
							value = Double.parseDouble(info);
						}

						// LOG.info("!!!! Get Memory Size:{}, info:{}", value,
						// info);
						return value;
					}
					if (m == 8) {
						// cpu usage
					}
					if (m == 9) {
						// memory ratio
					}
					m++;
				}
			} catch (Exception e) {
				LOGGER.warn("Failed to get memory usage", e);
			}
		}

		// this will be incorrect
		MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
		MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

		return (double) memoryUsage.getUsed();
	}

	
	static class SystemOperation {
		public static boolean isRoot() throws IOException {
	        String result = SystemOperation.exec("echo $EUID").substring(0, 1);
	        return Integer.valueOf(result.substring(0, result.length())) == 0;
	    }

	    public static void mount(String name, String target, String type, String data) throws IOException {
	        StringBuilder sb = new StringBuilder();
	        sb.append("mount -t ").append(type).append(" -o ").append(data).append(" ").append(name).append(" ").append(target);
	        SystemOperation.exec(sb.toString());
	    }

	    public static void umount(String name) throws IOException {
	        StringBuilder sb = new StringBuilder();
	        sb.append("umount ").append(name);
	        SystemOperation.exec(sb.toString());
	    }

	    public static String exec(String cmd) throws IOException {
	        List<String> commands = new ArrayList<>();
	        commands.add("/bin/bash");
	        commands.add("-c");
	        commands.add(cmd);

	        return JavaUtils.launchProcess(cmd, commands, new HashMap<String, String>(), false);
	    }
	}

}
