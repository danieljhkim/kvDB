package com.kvdb.kvclustercoordinator.handler;


import com.kvdb.kvclustercoordinator.cluster.ClusterManager;
import com.kvdb.kvclustercoordinator.cluster.ClusterNode;
import com.kvdb.kvclustercoordinator.protocol.ClusterCommandExecutor;
import com.kvdb.kvclustercoordinator.protocol.KVCommandParser;
import com.kvdb.kvcommon.exception.NoHealthyNodesAvailable;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles client connections using gRPC for communication with cluster nodes.
 */
public class ClusterClientHandler implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ClusterClientHandler.class.getName());
    private static final String KV_COMMAND = "KV";

    private final Socket clientSocket;
    private final String clientAddress;
    private final KVCommandParser commandParser;
    private final ClusterManager clusterManager;


    public ClusterClientHandler(Socket socket, ClusterManager clusterManager) {
        this.commandParser = new KVCommandParser();
        this.clientSocket = socket;
        this.clientAddress = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        this.clusterManager = clusterManager;
    }

    @Override
    public void run() {
        LOGGER.info("Client connected from " + clientAddress);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()))) {
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

    private void processClientCommands(BufferedReader reader, BufferedWriter writer) throws IOException, NoHealthyNodesAvailable {
        String command;
        while ((command = reader.readLine()) != null) {
            try {
                String[] parts = command.split(" ");
                if (parts.length == 0 || parts[0].isEmpty()) {
                    sendErrorResponse(writer, "Empty command received");
                    continue;
                }

                parts[0] = parts[0].toUpperCase();
                LOGGER.fine("Command received: " + command);

                if (parts[0].equals(KV_COMMAND)) {
                    ClusterNode node = clusterManager.getShardedNode(parts);
                    if (node == null) {
                        sendErrorResponse(writer, "Node not found");
                        continue;
                    }
                    LOGGER.info("Routing command to node: " + node.getId());
                    ClusterCommandExecutor executor = new ClusterCommandExecutor(node.getClient());
                    String response = commandParser.executeCommand(Arrays.copyOfRange(parts, 1, parts.length), executor);
                    writer.write(response + "\n");
                    writer.flush();
                    executor.shutdown();
                } else {
                    sendErrorResponse(writer, "commands not supported");
                }
            } catch (NoHealthyNodesAvailable e) {
                LOGGER.log(Level.WARNING, "No healthy nodes available", e);
                sendErrorResponse(writer, "No healthy nodes available");
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error processing command", e);
                sendErrorResponse(writer, "Internal server error");
            }
        }
    }

    private void sendErrorResponse(BufferedWriter writer, String message) {
        try {
            writer.write("ERROR: " + message + "\n");
            writer.flush();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to send error response", e);
        }
    }
}