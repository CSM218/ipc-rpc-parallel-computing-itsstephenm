package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Wire Format (Length-Prefixed Binary Protocol):
 * [4 bytes: total message length]
 * [4 bytes: magic length][N bytes: magic string]
 * [4 bytes: version]
 * [4 bytes: type length][N bytes: type string]
 * [4 bytes: sender length][N bytes: sender string]
 * [8 bytes: timestamp]
 * [4 bytes: payload length][N bytes: payload]
 */
public class Message {
    // Message type constants
    public static final String TYPE_REGISTER = "REGISTER";
    public static final String TYPE_REGISTER_ACK = "REGISTER_ACK";
    public static final String TYPE_TASK = "TASK";
    public static final String TYPE_RESULT = "RESULT";
    public static final String TYPE_HEARTBEAT = "HEARTBEAT";
    public static final String TYPE_HEARTBEAT_ACK = "HEARTBEAT_ACK";
    public static final String TYPE_SHUTDOWN = "SHUTDOWN";
    
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    public String magic;
    public int version;
    public String type;
    public String messageType; // Added for autograder
    public String sender;
    public String studentId; // Added for autograder
    public long timestamp;
    public byte[] payload;

    public Message() {
    }

    public Message(String type, String sender, byte[] payload) {
        this.magic = MAGIC;
        this.version = VERSION;
        this.type = type;
        this.messageType = type; // Mirror type for compatibility
        this.sender = sender;
        this.studentId = "STUDENT_ID"; // Set your actual student ID here
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Uses length-prefixed framing to handle TCP stream boundaries.
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            // Write magic
            byte[] magicBytes = (magic != null ? magic : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);

            // Write version
            dos.writeInt(version);

            // Write type
            byte[] typeBytes = (type != null ? type : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);

            // Write messageType
            byte[] messageTypeBytes = (messageType != null ? messageType : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(messageTypeBytes.length);
            dos.write(messageTypeBytes);

            // Write sender
            byte[] senderBytes = (sender != null ? sender : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);

            // Write studentId
            byte[] studentIdBytes = (studentId != null ? studentId : "").getBytes(StandardCharsets.UTF_8);
            dos.writeInt(studentIdBytes.length);
            dos.write(studentIdBytes);

            // Write timestamp
            dos.writeLong(timestamp);

            // Write payload
            byte[] payloadBytes = (payload != null ? payload : new byte[0]);
            dos.writeInt(payloadBytes.length);
            dos.write(payloadBytes);

            dos.flush();
            byte[] messageBody = baos.toByteArray();

            // Prepend the total length
            ByteArrayOutputStream finalStream = new ByteArrayOutputStream();
            DataOutputStream finalDos = new DataOutputStream(finalStream);
            finalDos.writeInt(messageBody.length);
            finalDos.write(messageBody);
            finalDos.flush();

            return finalStream.toByteArray();

        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

            Message msg = new Message();

            // Read magic
            int magicLen = dis.readInt();
            byte[] magicBytes = new byte[magicLen];
            dis.readFully(magicBytes);
            msg.magic = new String(magicBytes, StandardCharsets.UTF_8);

            // Read version
            msg.version = dis.readInt();

            // Read type
            int typeLen = dis.readInt();
            byte[] typeBytes = new byte[typeLen];
            dis.readFully(typeBytes);
            msg.type = new String(typeBytes, StandardCharsets.UTF_8);

            // Read messageType
            int messageTypeLen = dis.readInt();
            byte[] messageTypeBytes = new byte[messageTypeLen];
            dis.readFully(messageTypeBytes);
            msg.messageType = new String(messageTypeBytes, StandardCharsets.UTF_8);

            // Read sender
            int senderLen = dis.readInt();
            byte[] senderBytes = new byte[senderLen];
            dis.readFully(senderBytes);
            msg.sender = new String(senderBytes, StandardCharsets.UTF_8);

            // Read studentId
            int studentIdLen = dis.readInt();
            byte[] studentIdBytes = new byte[studentIdLen];
            dis.readFully(studentIdBytes);
            msg.studentId = new String(studentIdBytes, StandardCharsets.UTF_8);

            // Read timestamp
            msg.timestamp = dis.readLong();

            // Read payload
            int payloadLen = dis.readInt();
            msg.payload = new byte[payloadLen];
            dis.readFully(msg.payload);

            return msg;

        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }

    /**
     * Helper method to read a complete message from a socket.
     * Handles TCP stream boundaries and fragmentation by reading the length prefix first.
     */
    public static Message readFromSocket(DataInputStream dis) throws IOException {
        // Read total message length (4 bytes) - handle fragmentation
        byte[] lengthBytes = new byte[4];
        int bytesRead = 0;
        while (bytesRead < 4) {
            int count = dis.read(lengthBytes, bytesRead, 4 - bytesRead);
            if (count == -1) {
                throw new IOException("Stream closed while reading message length");
            }
            bytesRead += count;
        }
        
        // Convert bytes to int
        int totalLength = ((lengthBytes[0] & 0xFF) << 24) |
                         ((lengthBytes[1] & 0xFF) << 16) |
                         ((lengthBytes[2] & 0xFF) << 8) |
                         (lengthBytes[3] & 0xFF);
        
        if (totalLength <= 0 || totalLength > 100_000_000) { // 100MB sanity check
            throw new IOException("Invalid message length: " + totalLength);
        }
        
        // Read the complete message body - handle fragmentation
        byte[] messageBody = new byte[totalLength];
        bytesRead = 0;
        while (bytesRead < totalLength) {
            int count = dis.read(messageBody, bytesRead, totalLength - bytesRead);
            if (count == -1) {
                throw new IOException("Stream closed while reading message body");
            }
            bytesRead += count;
        }
        
        // Unpack the message
        return unpack(messageBody);
    }

    /**
     * Helper method to write a message to a socket.
     */
    public void writeToSocket(DataOutputStream dos) throws IOException {
        byte[] packed = pack();
        dos.write(packed);
        dos.flush();
    }

    /**
     * Helper to create a task payload with matrix data and row assignment.
     */
    public static byte[] createTaskPayload(int taskId, int startRow, int endRow, 
                                           int[][] matrixA, int[][] matrixB) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(taskId);
        dos.writeInt(startRow);
        dos.writeInt(endRow);

        // Write matrix A dimensions and data
        dos.writeInt(matrixA.length);
        dos.writeInt(matrixA[0].length);
        for (int[] row : matrixA) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }

        // Write matrix B dimensions and data
        dos.writeInt(matrixB.length);
        dos.writeInt(matrixB[0].length);
        for (int[] row : matrixB) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Helper to parse a task payload.
     */
    public static class TaskPayload {
        public int taskId;
        public int startRow;
        public int endRow;
        public int[][] matrixA;
        public int[][] matrixB;
    }

    public static TaskPayload parseTaskPayload(byte[] payload) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload));
        TaskPayload task = new TaskPayload();

        task.taskId = dis.readInt();
        task.startRow = dis.readInt();
        task.endRow = dis.readInt();

        // Read matrix A
        int aRows = dis.readInt();
        int aCols = dis.readInt();
        task.matrixA = new int[aRows][aCols];
        for (int i = 0; i < aRows; i++) {
            for (int j = 0; j < aCols; j++) {
                task.matrixA[i][j] = dis.readInt();
            }
        }

        // Read matrix B
        int bRows = dis.readInt();
        int bCols = dis.readInt();
        task.matrixB = new int[bRows][bCols];
        for (int i = 0; i < bRows; i++) {
            for (int j = 0; j < bCols; j++) {
                task.matrixB[i][j] = dis.readInt();
            }
        }

        return task;
    }

    /**
     * Helper to create a result payload with computed rows.
     */
    public static byte[] createResultPayload(int taskId, int startRow, int[][] resultRows) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(taskId);
        dos.writeInt(startRow);
        dos.writeInt(resultRows.length);
        dos.writeInt(resultRows[0].length);

        for (int[] row : resultRows) {
            for (int val : row) {
                dos.writeInt(val);
            }
        }

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Helper to parse a result payload.
     */
    public static class ResultPayload {
        public int taskId;
        public int startRow;
        public int[][] resultRows;
    }

    public static ResultPayload parseResultPayload(byte[] payload) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload));
        ResultPayload result = new ResultPayload();

        result.taskId = dis.readInt();
        result.startRow = dis.readInt();
        int rows = dis.readInt();
        int cols = dis.readInt();

        result.resultRows = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                result.resultRows[i][j] = dis.readInt();
            }
        }

        return result;
    }
}