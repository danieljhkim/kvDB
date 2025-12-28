package com.danieljhkim.kvdb.kvgateway.handler;

import com.danieljhkim.kvdb.kvgateway.cluster.ClusterManager;
import com.danieljhkim.kvdb.kvgateway.cluster.ClusterNode;
import com.danieljhkim.kvdb.kvgateway.protocol.ClusterCommandExecutor;
import com.danieljhkim.kvdb.kvgateway.protocol.KVCommandParser;
import com.danieljhkim.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles plain TCP client connections and routes KV commands to cluster nodes.
 */
public class ClusterClientHandler implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(ClusterClientHandler.class.getName());
	private static final String KV_COMMAND = "KV";

	private final Socket clientSocket;
	private final String clientAddress;
	private final KVCommandParser commandParser;
	private final ClusterManager clusterManager;
	private final ClusterCommandExecutor executor = new ClusterCommandExecutor();

	public ClusterClientHandler(Socket socket, ClusterManager clusterManager) {
		this.commandParser = new KVCommandParser();
		this.clientSocket = socket;
		this.clientAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
		this.clusterManager = clusterManager;
	}

	@Override
	public void run() {
		LOGGER.info("Client connected from " + clientAddress);

		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(
						clientSocket.getInputStream(), StandardCharsets.UTF_8));
				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(
								clientSocket.getOutputStream(), StandardCharsets.UTF_8))) {

			processClientCommands(reader, writer);

		} catch (IOException e) {
			LOGGER.log(Level.WARNING, "Error handling client " + clientAddress, e);
		} finally {
			try {
				clientSocket.close();
			} catch (IOException e) {
				LOGGER.log(Level.WARNING, "Error closing client socket", e);
			}
			LOGGER.info("Connection closed with client " + clientAddress);
		}
	}

	private void processClientCommands(BufferedReader reader, BufferedWriter writer)
			throws IOException {
		String command;
		while ((command = reader.readLine()) != null) {
			try {
				// Normalize whitespace and split into tokens
				String[] parts = command.trim().split("\\s+");
				if (parts.length == 0 || parts[0].isEmpty()) {
					sendErrorResponse(writer, "Empty command received");
					continue;
				}

				parts[0] = parts[0].toUpperCase();
				LOGGER.fine("Command received from " + clientAddress + ": " + command);

				if (KV_COMMAND.equals(parts[0])) {
					ClusterNode node = clusterManager.getShardedNode(parts);
					if (node == null) {
						sendErrorResponse(writer, "Node not found");
						continue;
					}

					LOGGER.fine("Routing command to node: " + node.getId());
					executor.setClient(node.getClient());

					String response = commandParser.executeCommand(
							Arrays.copyOfRange(parts, 1, parts.length), executor);
					writer.write(response);
					writer.write("\n");
					writer.flush();
				} else {
					sendErrorResponse(writer, "Command not supported: " + parts[0]);
				}

			} catch (IllegalArgumentException e) {
				LOGGER.log(
						Level.WARNING, "Invalid command from " + clientAddress + ": " + command, e);
				sendErrorResponse(writer, "Invalid command: " + e.getMessage());
			} catch (NoHealthyNodesAvailable e) {
				LOGGER.log(
						Level.WARNING, "No healthy nodes available for client " + clientAddress, e);
				sendErrorResponse(writer, "No healthy nodes available");
			} catch (Exception e) {
				LOGGER.log(
						Level.WARNING,
						"Error processing command from " + clientAddress + ": " + command,
						e);
				sendErrorResponse(writer, "Internal server error: " + e.getMessage());
			}
		}
	}

	private void sendErrorResponse(BufferedWriter writer, String message) {
		try {
			writer.write("ERROR: " + message);
			writer.write("\n");
			writer.flush();
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Failed to send error response to " + clientAddress, e);
		}
	}
}
