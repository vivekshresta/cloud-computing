package client;


import helper.LogHelper;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
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

    public List<String[]> getKVPairs(String fileID) {
        String command = READ_FILE_COMMAND + " " + fileID + ".txt";
        oLog.info("Command to get KV pairs: " + command);
        List<String[]> kvPairs = new ArrayList<>();

        String fileContents = execute(command, true);
        StringTokenizer linesTokenizer = new StringTokenizer(fileContents, "\n");
        while(linesTokenizer.hasMoreElements()) {
            String[] kvPair = linesTokenizer.nextToken().split(" ");
            kvPairs.add(kvPair);
        }

        return kvPairs;
    }

    public String appendToKVStore(String fileID, String key, String value) {
        String command = APPEND_TO_FILE_COMMAND + " " + fileID + ".txt " + key + " " + value;
        oLog.info("Command to append to KV store: " + command);
        //String command = String.format("set %s 9 0 %s noreply \\r\\n%s \\r\\n", key, value.length(), value);
        return execute(command, false);
    }

    private String execute(String command, boolean outputRequired) {
        StringBuilder output = new StringBuilder();
        try {
            outToServer.writeUTF(command);

            if(outputRequired) {
                String line;
                while (!(line = inFromServer.readUTF()).contains(END_OF_TRANSMISSION))
                    output.append(line);
            }

        } catch (Exception e) {
            oLog.warning("SERVER_ERROR <" + e.getMessage() + ">");
        }

        return output.toString();
    }

    public void destroy() {
        try {
            inFromServer.close();
            outToServer.close();
            clientSocket.close();
        } catch (IOException e) {
            oLog.warning(Arrays.toString(e.getStackTrace()));
        }
    }
}
