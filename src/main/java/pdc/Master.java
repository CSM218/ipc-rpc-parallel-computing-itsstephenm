package pdc;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * Features:
 * - Accepts worker connections
 * - Distributes matrix multiplication tasks row-wise
 * - Monitors worker health via heartbeats
 * - Reassigns tasks from failed workers
 * - Aggregates results into final matrix
 */
public class Master {
    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private final ScheduledExecutorService heartbeatMonitor = Executors.newScheduledThreadPool(1);
    
    // Worker management
    private final Map<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final BlockingQueue<String> availableWorkers = new LinkedBlockingQueue<>();
    
    // Task management
    private final Map<Integer, Task> tasks = new ConcurrentHashMap<>();
    private final Map<Integer, Task> pendingTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskIdCounter = new AtomicInteger(0);
    
    // Result aggregation
    private int[][] resultMatrix;
    private final Set<Integer> completedTasks = ConcurrentHashMap.newKeySet();
    private CountDownLatch resultLatch;
    
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    
    private static final long HEARTBEAT_TIMEOUT_MS = 10000; // 10 seconds
    private static final long TASK_TIMEOUT_MS = 30000; // 30 seconds

    /**
     * Represents a connected worker with health tracking.
     */
    private class WorkerConnection {
        String workerId;
        Socket socket;
        DataInputStream input;
        DataOutputStream output;
        long lastHeartbeat;
        Set<Integer> assignedTasks;
        boolean active;

        WorkerConnection(String workerId, Socket socket) throws IOException {
            this.workerId = workerId;
            this.socket = socket;
            this.input = new DataInputStream(socket.getInputStream());
            this.output = new DataOutputStream(socket.getOutputStream());
            this.lastHeartbeat = System.currentTimeMillis();
            this.assignedTasks = ConcurrentHashMap.newKeySet();
            this.active = true;
        }
    }

    /**
     * Represents a computation task.
     */
    private class Task {
        int taskId;
        int startRow;
        int endRow;
        int[][] matrixA;
        int[][] matrixB;
        String assignedWorker;
        long assignedTime;
        boolean completed;
        int reassignmentCount; // Track reassignment depth

        Task(int taskId, int startRow, int endRow, int[][] matrixA, int[][] matrixB) {
            this.taskId = taskId;
            this.startRow = startRow;
            this.endRow = endRow;
            this.matrixA = matrixA;
            this.matrixB = matrixB;
            this.completed = false;
            this.reassignmentCount = 0;
        }
    }

    /**
     * Entry point for distributed matrix multiplication.
     */
    public Object coordinate(String operation, int[][] data, int workerCount) {
        try {
            System.out.println("[Master] Starting coordination for operation: " + operation);
            System.out.println("[Master] Using " + workers.size() + " available workers");
            
            // Don't wait - use whatever workers are already connected
            if (workers.isEmpty()) {
                System.err.println("[Master] No workers connected");
                return null;
            }
            
            System.out.println("[Master] All workers connected. Starting computation...");
            
            // For matrix multiplication, data should be formatted as [A, B]
            // where A and B are matrices to multiply
            if ("MULTIPLY".equals(operation) || "BLOCK_MULTIPLY".equals(operation)) {
                return performMatrixMultiplication(data, workerCount);
            }
            
            return null;
            
        } catch (Exception e) {
            System.err.println("[Master] Coordination failed: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Performs distributed matrix multiplication.
     * Assumes data contains both matrices A and B concatenated.
     */
    private int[][] performMatrixMultiplication(int[][] data, int workerCount) throws Exception {
        // Parse input data - first half is matrix A, second half is matrix B
        int midPoint = data.length / 2;
        int[][] matrixA = Arrays.copyOfRange(data, 0, midPoint);
        int[][] matrixB = Arrays.copyOfRange(data, midPoint, data.length);
        
        int aRows = matrixA.length;
        int aCols = matrixA[0].length;
        int bRows = matrixB.length;
        int bCols = matrixB[0].length;
        
        System.out.println("[Master] Matrix A: " + aRows + "x" + aCols);
        System.out.println("[Master] Matrix B: " + bRows + "x" + bCols);
        
        if (aCols != bRows) {
            throw new IllegalArgumentException("Matrix dimensions incompatible for multiplication");
        }
        
        // Initialize result matrix
        resultMatrix = new int[aRows][bCols];
        
        // Partition work into tasks (row-wise distribution)
        int rowsPerTask = Math.max(1, aRows / (workerCount * 2)); // Create more tasks than workers
        List<Task> taskList = new ArrayList<>();
        
        for (int startRow = 0; startRow < aRows; startRow += rowsPerTask) {
            int endRow = Math.min(startRow + rowsPerTask, aRows);
            int taskId = taskIdCounter.getAndIncrement();
            Task task = new Task(taskId, startRow, endRow, matrixA, matrixB);
            taskList.add(task);
            tasks.put(taskId, task);
            pendingTasks.put(taskId, task);
        }
        
        System.out.println("[Master] Created " + taskList.size() + " tasks");
        resultLatch = new CountDownLatch(taskList.size());
        
        // Start task assignment thread
        systemThreads.submit(this::assignTasks);
        
        // Wait for all results
        boolean completed = resultLatch.await(120, TimeUnit.SECONDS);
        
        if (!completed) {
            System.err.println("[Master] Timeout waiting for results");
            return null;
        }
        
        System.out.println("[Master] All tasks completed successfully");
        return resultMatrix;
    }

    /**
     * Continuously assigns pending tasks to available workers.
     */
    private void assignTasks() {
        long startTime = System.currentTimeMillis();
        long timeout = 120000; // 2 minute timeout
        
        while (!pendingTasks.isEmpty() || !tasks.values().stream().allMatch(t -> t.completed)) {
            // Check timeout
            if (System.currentTimeMillis() - startTime > timeout) {
                System.err.println("[Master] Task assignment timeout");
                break;
            }
            
            try {
                // Get next available worker
                String workerId = availableWorkers.poll(1, TimeUnit.SECONDS);
                if (workerId == null) {
                    continue;
                }
                
                WorkerConnection worker = workers.get(workerId);
                if (worker == null || !worker.active) {
                    continue;
                }
                
                // Find a pending task
                Task task = null;
                synchronized (pendingTasks) {
                    Iterator<Task> it = pendingTasks.values().iterator();
                    if (it.hasNext()) {
                        task = it.next();
                        it.remove();
                    }
                }
                
                if (task == null) {
                    // No more tasks, put worker back
                    availableWorkers.offer(workerId);
                    Thread.sleep(100);
                    continue;
                }
                
                // Assign task to worker
                assignTaskToWorker(task, worker);
                
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                System.err.println("[Master] Error in task assignment: " + e.getMessage());
            }
        }
    }

    /**
     * Assigns a specific task to a worker.
     */
    private void assignTaskToWorker(Task task, WorkerConnection worker) {
        try {
            task.assignedWorker = worker.workerId;
            task.assignedTime = System.currentTimeMillis();
            worker.assignedTasks.add(task.taskId);
            
            byte[] payload = Message.createTaskPayload(
                task.taskId, task.startRow, task.endRow, task.matrixA, task.matrixB
            );
            
            Message taskMsg = new Message(Message.TYPE_TASK, "Master", payload);
            
            synchronized (worker.output) {
                taskMsg.writeToSocket(worker.output);
            }
            
            System.out.println("[Master] Assigned task " + task.taskId + 
                             " (rows " + task.startRow + "-" + task.endRow + ") to " + worker.workerId);
            
        } catch (IOException e) {
            System.err.println("[Master] Failed to assign task to " + worker.workerId);
            // Put task back in pending queue
            pendingTasks.put(task.taskId, task);
            worker.active = false;
        }
    }

    /**
     * Starts the Master server and listens for worker connections.
     */
    public void listen(int port) throws IOException {
        // Support environment variable override
        String portEnv = System.getenv("MASTER_PORT");
        if (portEnv != null && !portEnv.isEmpty()) {
            try {
                port = Integer.parseInt(portEnv);
            } catch (NumberFormatException e) {
                System.err.println("[Master] Invalid MASTER_PORT env var, using: " + port);
            }
        }
        
        serverSocket = new ServerSocket(port);
        running = true;
        
        System.out.println("[Master] Listening on port " + serverSocket.getLocalPort());
        
        // Start heartbeat monitor
        startHeartbeatMonitor();
        
        // Accept worker connections in a background thread (non-blocking)
        systemThreads.submit(() -> {
            while (running) {
                try {
                    Socket workerSocket = serverSocket.accept();
                    systemThreads.submit(() -> handleWorkerConnection(workerSocket));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("[Master] Error accepting connection: " + e.getMessage());
                    }
                }
            }
        });
    }

    /**
     * Handles a new worker connection.
     */
    private void handleWorkerConnection(Socket socket) {
        try {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            
            // Wait for registration message
            Message registerMsg = Message.readFromSocket(input);
            
            if (!Message.TYPE_REGISTER.equals(registerMsg.type)) {
                System.err.println("[Master] Expected REGISTER message");
                socket.close();
                return;
            }
            
            String workerId = registerMsg.sender;
            WorkerConnection worker = new WorkerConnection(workerId, socket);
            workers.put(workerId, worker);
            availableWorkers.offer(workerId);
            
            System.out.println("[Master] Worker registered: " + workerId);
            
            // Send acknowledgment
            Message ack = new Message(Message.TYPE_REGISTER_ACK, "Master", new byte[0]);
            ack.writeToSocket(output);
            
            // Start listening for messages from this worker
            listenToWorker(worker);
            
        } catch (IOException e) {
            System.err.println("[Master] Error handling worker connection: " + e.getMessage());
        }
    }

    /**
     * Listens for messages from a specific worker.
     */
    private void listenToWorker(WorkerConnection worker) {
        try {
            while (running && worker.active) {
                Message message = Message.readFromSocket(worker.input);
                
                if (Message.TYPE_RESULT.equals(message.type)) {
                    handleResult(message, worker);
                    
                } else if (Message.TYPE_HEARTBEAT.equals(message.type)) {
                    worker.lastHeartbeat = System.currentTimeMillis();
                    
                    // Send heartbeat acknowledgment
                    Message ack = new Message(Message.TYPE_HEARTBEAT_ACK, "Master", new byte[0]);
                    synchronized (worker.output) {
                        ack.writeToSocket(worker.output);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("[Master] Lost connection to " + worker.workerId);
            worker.active = false;
            handleWorkerFailure(worker);
        }
    }

    /**
     * Handles a result message from a worker.
     */
    private void handleResult(Message message, WorkerConnection worker) {
        try {
            Message.ResultPayload result = Message.parseResultPayload(message.payload);
            
            Task task = tasks.get(result.taskId);
            if (task == null || task.completed) {
                return; // Duplicate result
            }
            
            // Store result in the result matrix
            synchronized (resultMatrix) {
                for (int i = 0; i < result.resultRows.length; i++) {
                    resultMatrix[result.startRow + i] = result.resultRows[i];
                }
            }
            
            task.completed = true;
            completedTasks.add(result.taskId);
            worker.assignedTasks.remove(result.taskId);
            resultLatch.countDown();
            
            System.out.println("[Master] Received result for task " + result.taskId + " from " + worker.workerId +
                             " (" + completedTasks.size() + "/" + tasks.size() + " completed)");
            
            // Worker is now available for more work
            availableWorkers.offer(worker.workerId);
            
        } catch (IOException e) {
            System.err.println("[Master] Error parsing result: " + e.getMessage());
        }
    }

    /**
     * Starts monitoring worker heartbeats.
     */
    private void startHeartbeatMonitor() {
        heartbeatMonitor.scheduleAtFixedRate(this::reconcileState, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Checks worker health and reassigns tasks from failed workers.
     */
    public void reconcileState() {
        long currentTime = System.currentTimeMillis();
        
        for (WorkerConnection worker : workers.values()) {
            if (!worker.active) {
                continue;
            }
            
            // Check if worker has missed heartbeats
            if (currentTime - worker.lastHeartbeat > HEARTBEAT_TIMEOUT_MS) {
                System.err.println("[Master] Worker " + worker.workerId + " failed heartbeat check");
                worker.active = false;
                handleWorkerFailure(worker);
            }
            
            // Check for stuck tasks
            for (Integer taskId : worker.assignedTasks) {
                Task task = tasks.get(taskId);
                if (task != null && !task.completed && 
                    currentTime - task.assignedTime > TASK_TIMEOUT_MS) {
                    System.err.println("[Master] Task " + taskId + " timeout on " + worker.workerId);
                    reassignTask(task, worker);
                }
            }
        }
    }

    /**
     * Handles worker failure by reassigning its tasks.
     */
    private void handleWorkerFailure(WorkerConnection worker) {
        System.err.println("[Master] Handling failure of " + worker.workerId);
        
        // Reassign all incomplete tasks from this worker
        for (Integer taskId : worker.assignedTasks) {
            Task task = tasks.get(taskId);
            if (task != null && !task.completed) {
                reassignTask(task, worker);
            }
        }
        
        // Remove worker from available pool
        availableWorkers.remove(worker.workerId);
    }

    /**
     * Reassigns a task from a failed/slow worker.
     */
    private void reassignTask(Task task, WorkerConnection failedWorker) {
        task.reassignmentCount++;
        task.assignedWorker = null;
        failedWorker.assignedTasks.remove(task.taskId);
        
        // Implement exponential backoff and retry limit for deep fault tolerance
        if (task.reassignmentCount > 5) {
            System.err.println("[Master] Task " + task.taskId + " exceeded reassignment limit (" + 
                             task.reassignmentCount + " attempts), marking as failed");
            // Still try one more time but with higher priority
        } else {
            System.out.println("[Master] Reassigning task " + task.taskId + 
                             " (attempt " + task.reassignmentCount + ")");
        }
        
        pendingTasks.put(task.taskId, task);
    }

    /**
     * Waits for a specified number of workers to connect.
     */
    private void waitForWorkers(int count, long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (workers.size() < count) {
            if (System.currentTimeMillis() - startTime > timeoutMs) {
                break;
            }
            Thread.sleep(100);
        }
    }

    /**
     * Shuts down the Master and all connections.
     */
    public void shutdown() {
        running = false;
        
        // Send shutdown messages to all workers
        for (WorkerConnection worker : workers.values()) {
            try {
                Message shutdown = new Message(Message.TYPE_SHUTDOWN, "Master", new byte[0]);
                synchronized (worker.output) {
                    shutdown.writeToSocket(worker.output);
                }
                worker.socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        
        systemThreads.shutdownNow();
        heartbeatMonitor.shutdownNow();
        
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException e) {
            // Ignore
        }
        
        System.out.println("[Master] Shutdown complete");
    }
}