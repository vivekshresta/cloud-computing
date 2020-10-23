package client;


import helper.LogHelper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

public class Client {
    private static final Logger oLog = LogHelper.getLogger(Client.class.getName());
    private final String READ_FILE_COMMAND = "readfile";
    private final String APPEND_TO_FILE_COMMAND = "appendtofile";
    private final String END_OF_TRANSMISSION = "END_OF_TRANSMISSION";

    private final String inetAddress;
    private final int port;

    private final Socket clientSocket;
    private final DataInputStream inFromServer;
    private final DataOutputStream outToServer;

    public Client(String inetAddress, int port) throws IOException {
        this.inetAddress = inetAddress;
        this.port = port;

        clientSocket = new Socket(inetAddress, port);
        inFromServer = new DataInputStream(clientSocket.getInputStream());
        outToServer = new DataOutputStream(clientSocket.getOutputStream());
    }

    public String getFileData(String fileID) {
        String command = READ_FILE_COMMAND + " " + fileID + ".txt";
        oLog.info("Command to get file data: " + command);
        return execute(command);
    }

    public String createDataChunk(String dataChunk, String fileID) {
        if(isBlank(dataChunk))
            return "";

        String command = APPEND_TO_FILE_COMMAND + " " + fileID + ".txt" + " " + dataChunk;
        oLog.info("Command to create data chunk: " + command);
        return execute(command);
    }

    private String execute(String command) {
        StringBuilder output = new StringBuilder();
        try {
            outToServer.writeUTF(command);

            String line;
            while(!(line = inFromServer.readUTF()).contains(END_OF_TRANSMISSION))
                output.append(line);

        } catch (Exception e) {
            oLog.warning("SERVER_ERROR <" + e.getMessage() + ">");
        }

        return output.toString();
    }

    private boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    public void destroy() {
        try {
            inFromServer.close();
            outToServer.close();
            clientSocket.close();
        } catch (IOException e) {
            oLog.warning("Exception: " + e.getMessage());
        }
    }

    public String getInetAddress() {
        return inetAddress;
    }

    public int getPort() {
        return port;
    }
}
