package com.danieljhkim.kvdb.kvclient;

import com.danieljhkim.kvdb.kvclient.cli.CLIClient;
import com.danieljhkim.kvdb.kvcommon.config.SystemConfig;

public class KvClientRunner {
	private static final String DEFAULT_HOST = "localhost";
	private static final int DEFAULT_PORT = 7000;

	public static void main(String[] args) {
		SystemConfig config = SystemConfig.getInstance();
		String host = config.getProperty("kvdb.server.host", DEFAULT_HOST);
		int port = DEFAULT_PORT;

		try {
			port = Integer.parseInt(config.getProperty("kvdb.server.port", String.valueOf(DEFAULT_PORT)));
		} catch (NumberFormatException ex) {
			System.out.println("Warning: Invalid port number in configuration. Using default port.");
		}

		// passed with -D flags
		String sysHost = System.getProperty("kvclient.host");
		String sysPort = System.getProperty("kvclient.port");

		if (sysHost != null && !sysHost.isEmpty()) {
			host = sysHost;
		}

		if (sysPort != null && !sysPort.isEmpty()) {
			try {
				port = Integer.parseInt(sysPort);
			} catch (NumberFormatException ex) {
				System.out.println("Warning: Invalid command-line port. Using configured port.");
			}
		}

		System.out.println("Connecting to " + host + ":" + port);
		CLIClient client = new CLIClient();
		boolean connected = client.connect(host, port);

		if (connected) {
			client.runCli();
			client.disconnect();
		} else {
			System.out.println("Exiting due to connection failure.");
			System.exit(1);
		}
	}
}