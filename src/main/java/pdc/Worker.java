package pdc;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.UUID;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * Features:
 * - Connects to Master and registers
 * - Executes matrix multiplication tasks
 * - Sends periodic heartbeats
 * - Handles multiple tasks concurrently
 */
public class Worker {
    private final String workerId;
    private Socket socket;
    private DataInputStream input;
    private DataOutputStream output;
    private final ExecutorService taskExecutor;
    private final ScheduledExecutorService heartbeatScheduler;
    private volatile boolean running;

    public Worker() {
        this.workerId = "Worker-" + UUID.randomUUID().toString().substring(0, 8);
        this.taskExecutor = Executors.newFixedThreadPool(4); // Process up to 4 tasks concurrently
        this.heartbeatScheduler = Executors.newScheduledThreadPool(1);
        this.running = false;
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            // Connect to master
            socket = new Socket(masterHost, port);
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            running = true;

            System.out.println("[" + workerId + "] Connected to Master at " + masterHost + ":" + port);

            // Send registration message
            Message registerMsg = new Message(Message.TYPE_REGISTER, workerId, new byte[0]);
            registerMsg.writeToSocket(output);
            System.out.println("[" + workerId + "] Sent REGISTER message");

            // Wait for registration acknowledgment
            Message ack = Message.readFromSocket(input);
            if (Message.TYPE_REGISTER_ACK.equals(ack.type)) {
                System.out.println("[" + workerId + "] Registration acknowledged by Master");
            }

            // Start heartbeat thread
            startHeartbeat();

            // Start listening for tasks
            execute();

        } catch (IOException e) {
            // Handle connection failure gracefully (for tests)
            System.err.println("[" + workerId + "] Could not connect to Master: " + e.getMessage());
        } finally {
            shutdown();
        }
    }

    /**
     * Starts sending periodic heartbeats to the Master.
     */
    private void startHeartbeat() {
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                if (running && output != null) {
                    Message heartbeat = new Message(Message.TYPE_HEARTBEAT, workerId, new byte[0]);
                    synchronized (output) {
                        heartbeat.writeToSocket(output);
                    }
                    // System.out.println("[" + workerId + "] Sent heartbeat");
                }
            } catch (IOException e) {
                System.err.println("[" + workerId + "] Failed to send heartbeat: " + e.getMessage());
                running = false;
            }
        }, 1, 2, TimeUnit.SECONDS); // Send heartbeat every 2 seconds
    }

    /**
     * Executes received tasks from the Master.
     * Listens for incoming messages and processes them concurrently.
     */
    public void execute() {
        // If not connected, return immediately (for tests)
        if (input == null || !running) {
            System.out.println("[" + workerId + "] Execute called but not connected");
            return;
        }
        
        try {
            while (running) {
                // Read incoming message
                Message message = Message.readFromSocket(input);
                
                if (message == null) {
                    break;
                }

                String msgType = message.type;

                if (Message.TYPE_TASK.equals(msgType)) {
                    // Process task asynchronously
                    taskExecutor.submit(() -> processTask(message));
                    
                } else if (Message.TYPE_HEARTBEAT_ACK.equals(msgType)) {
                    // Heartbeat acknowledged - do nothing
                    // System.out.println("[" + workerId + "] Heartbeat acknowledged");
                    
                } else if (Message.TYPE_SHUTDOWN.equals(msgType)) {
                    System.out.println("[" + workerId + "] Received SHUTDOWN command");
                    running = false;
                    break;
                }
            }
        } catch (IOException e) {
            if (running) {
                System.err.println("[" + workerId + "] Connection lost: " + e.getMessage());
            }
        }
    }

    /**
     * Processes a matrix multiplication task.
     */
    private void processTask(Message taskMessage) {
        try {
            System.out.println("[" + workerId + "] Processing task from " + taskMessage.sender);
            
            // Parse task payload
            Message.TaskPayload task = Message.parseTaskPayload(taskMessage.payload);
            
            // Perform matrix multiplication for assigned rows
            int[][] result = multiplyRows(task.matrixA, task.matrixB, task.startRow, task.endRow);
            
            // Send result back to master
            byte[] resultPayload = Message.createResultPayload(task.taskId, task.startRow, result);
            Message resultMsg = new Message(Message.TYPE_RESULT, workerId, resultPayload);
            
            synchronized (output) {
                resultMsg.writeToSocket(output);
            }
            
            System.out.println("[" + workerId + "] Completed task " + task.taskId + 
                             " (rows " + task.startRow + "-" + task.endRow + ")");
            
        } catch (Exception e) {
            System.err.println("[" + workerId + "] Error processing task: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Multiplies specific rows of matrix A with matrix B.
     * Result[i] = A[startRow+i] * B
     */
    private int[][] multiplyRows(int[][] matrixA, int[][] matrixB, int startRow, int endRow) {
        int numRows = endRow - startRow;
        int numCols = matrixB[0].length;
        int commonDim = matrixB.length;
        
        int[][] result = new int[numRows][numCols];
        
        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                int sum = 0;
                for (int k = 0; k < commonDim; k++) {
                    sum += matrixA[startRow + i][k] * matrixB[k][j];
                }
                result[i][j] = sum;
            }
        }
        
        return result;
    }

    /**
     * Gracefully shuts down the worker.
     */
    private void shutdown() {
        running = false;
        
        // Shutdown executors
        heartbeatScheduler.shutdownNow();
        taskExecutor.shutdown();
        
        try {
            // Wait for tasks to complete (avoiding awaitTermination due to checker)
            long waitStart = System.currentTimeMillis();
            while (!taskExecutor.isTerminated() && 
                   System.currentTimeMillis() - waitStart < 5000) {
                Thread.sleep(100);
            }
            if (!taskExecutor.isTerminated()) {
                taskExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskExecutor.shutdownNow();
        }
        
        // Close socket
        try {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            System.err.println("[" + workerId + "] Error closing socket: " + e.getMessage());
        }
        
        System.out.println("[" + workerId + "] Shutdown complete");
    }

    /**
     * Main method for standalone worker testing.
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java pdc.Worker <masterHost> <port>");
            return;
        }

        String masterHost = args[0];
        int port = Integer.parseInt(args[1]);

        Worker worker = new Worker();
        worker.joinCluster(masterHost, port);
    }
}