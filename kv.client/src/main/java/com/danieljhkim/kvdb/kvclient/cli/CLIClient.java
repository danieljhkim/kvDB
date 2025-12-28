package com.danieljhkim.kvdb.kvclient.cli;

import com.danieljhkim.kvdb.kvclient.utils.Constants;

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Objects;
import java.util.Scanner;

/**
 * Client for interacting with the Redis-like key-value store server via command
 * line.
 */
public class CLIClient {

	private final Scanner scanner;
	private String host;
	private int port;
	private Socket socket;
	private BufferedReader reader;
	private BufferedWriter writer;
	private boolean connected = false;

	public CLIClient() {
		this.scanner = new Scanner(System.in);
	}

	public void runCli() {
		if (!isConnected()) {
			System.out.println("Error: Not connected to server. Please connect first.");
			return;
		}
		try {
			while (true) {
				System.out.print(Constants.PROMPT);
				String command = scanner.nextLine().trim();

				if (command.equalsIgnoreCase("exit") || command.equalsIgnoreCase("quit")) {
					break;
				}
				if (command.isEmpty()) {
					continue;
				}
				String response = executeCommand(command);
				System.out.println(response);
			}
		} catch (IOException e) {
			System.err.println("Connection error: " + e.getMessage());
			disconnect();
		}
	}

	public boolean connect(String host, int port) {
		if (isConnected()) {
			disconnect();
		}

		Objects.requireNonNull(host, "Host cannot be null");
		if (port <= 0 || port > 65535) {
			throw new IllegalArgumentException("Invalid port number: " + port);
		}
		this.host = host;
		this.port = port;
		try {
			this.socket = new Socket(host, port);
			this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			this.connected = true;
			System.out.println(Constants.WELCOME_MESSAGE);
			System.out.println("Connected to KV Database on " + host + ":" + port);
			System.out.println(Constants.LINE_SEPARATOR);
			System.out.println(Constants.KV_HELP_MESSAGE);
			System.out.println(Constants.LINE_SEPARATOR);
			return true;
		} catch (ConnectException e) {
			System.err.println("Failed to connect to server: " + e.getMessage());
			return false;
		} catch (IOException e) {
			System.err.println("Error establishing connection: " + e.getMessage());
			return false;
		}
	}

	public void disconnect() {
		try {
			if (isConnected()) {
				writer.close();
				reader.close();
				socket.close();
				System.out.println("Disconnected from server.");
			}
		} catch (IOException e) {
			System.err.println("Error during disconnect: " + e.getMessage());
		} finally {
			this.socket = null;
			this.reader = null;
			this.writer = null;
			this.connected = false;
		}
	}

	public String sendCommand(String command, String... args) throws IOException {
		if (!isConnected()) {
			return "Error: Not connected to server";
		}

		StringBuilder commandBuilder = new StringBuilder(command);
		for (String arg : args) {
			commandBuilder.append(" ").append(arg);
		}

		return executeCommand(commandBuilder.toString());
	}

	private String executeCommand(String commandString) throws IOException {
		writer.write(commandString + "\n");
		writer.flush();

		StringBuilder response = new StringBuilder();
		String line;
		socket.setSoTimeout(3000);
		try {
			while ((line = reader.readLine()) != null) {
				if (line.equals(Constants.END_MARKER)) {
					break;
				}
				if (!response.isEmpty()) {
					response.append("\n");
				}
				response.append(line);
				if (!reader.ready()) {
					break;
				}
			}
		} catch (IOException e) {
			if (e.getMessage().contains("Read timed out")) {
				System.err.println("Server response timed out.");
			} else {
				throw e;
			}
		} finally {
			socket.setSoTimeout(0);
		}

		return response.toString();
	}

	public boolean isConnected() {
		return connected && socket != null && !socket.isClosed();
	}
}