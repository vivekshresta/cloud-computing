package server;


import helper.LogHelper;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class CommandExecutor extends Thread {
    private static final Logger oLog = LogHelper.getLogger(CommandExecutor.class.getName());

    protected static final String NOREPLY = "noreply";
    protected static final String SET = "set";
    protected static final String ADD = "add";
    protected static final String REPLACE = "replace";
    protected static final String APPEND = "append";
    protected static final String PREPEND = "prepend";
    protected static final String CAS = "cas";
    protected static final String GET = "get";
    protected static final String GETS = "gets";
    protected static final String DELETE = "delete";

    protected static final String WRITE_TO_FILE = "writetofile";
    protected static final String READ_FILE = "readfile";
    protected static final String APPEND_TO_FILE = "appendtofile";
    protected static final String END_OF_TRANSMISSION = "END_OF_TRANSMISSION";

    private DataOutputStream out;
    private String command;
    private Socket socket;
    private CommandParser parser;
    private static ConcurrentHashMap<String, String> valuesStore = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, String> flagsStore = new ConcurrentHashMap<>();

    public CommandExecutor(Socket socket) {
        this.socket = socket;
        parser = new CommandParser();
    }

    public synchronized void run() {
        oLog.info("New client connection. Connected with Thread:" + Thread.currentThread().getName() + " through " + socket);
        while (!isInterrupted()) {
            try {
                synchronized (this) {
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    out = new DataOutputStream(socket.getOutputStream());
                    command = in.readUTF();
                }
            } catch (Exception e) {
                oLog.info("SERVER_ERROR <Socket error>\r\n");
                sendMessage("SERVER_ERROR <Socket error>\r\n");
                return;
            }

            try {
                if(!populateMapFromFile(valuesStore, Server.VALUES_CSV)) // retrieves key value store data and puts it into map
                    return;
                if(!populateMapFromFile(flagsStore, Server.FLAGS_CSV)) // retrieves flags data and puts into map
                    return;
            } catch (IOException ex) {
                oLog.warning(Arrays.toString(ex.getStackTrace()));
            }


            synchronized (this) {
                if (command.contains(NOREPLY))
                    parser.noReply = true;

                StringTokenizer tokenizer = new StringTokenizer(command);
                String command;
                if (tokenizer.hasMoreTokens()) {
                    command = tokenizer.nextToken();
                    switch (command) {
                        /* Storage Commands */
                        case SET: {    // "set" stores the data
                            oLog.info("Executing set command");
                            executeSet(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        case ADD: {    // "add" stores the data only if the server already doesn't have this data
                            oLog.info("Executing add command");
                            executeAdd(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        case REPLACE: {    // "replace" replaces the data only if the server doesn't already have it
                            oLog.info("Executing replace command");
                            executeReplace(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        case APPEND: {     // "append" adds this data to an existing key after the existing data
                            oLog.info("Executing append command");
                            executeAppend(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        case PREPEND: {    // "prepend" adds the data to existing data
                            oLog.info("Executing prepend command");
                            executePrepend(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        case CAS: {    // "cas" is an operation that stores data, only if no one else has updated the data since I last read it
                            oLog.info("Executing cas command");
                            executeCas(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        /* Retrieval Commands */
                        case GET:
                        case GETS: {
                            oLog.info("Executing get command");
                            executeGet(tokenizer);
                            break;
                        }
                        case DELETE: {      // The command "delete" allows for explicit deletion of items
                            oLog.info("Executing delete command");
                            executeDelete(tokenizer);
                            if (!updateFile(valuesStore, Server.VALUES_CSV))
                                return;
                            if (!updateFile(flagsStore, Server.FLAGS_CSV))
                                return;
                            break;
                        }
                        /* New commands for Map reduce */
                        case WRITE_TO_FILE: {      // The command "delete" allows for explicit deletion of items
                            oLog.info("Executing writetofile command");
                            executeWriteToFile(tokenizer);
                            break;
                        }
                        case READ_FILE: {      // The command "delete" allows for explicit deletion of items
                            oLog.info("Executing readfile command");
                            executeReadFile(tokenizer);
                            break;
                        }
                        case APPEND_TO_FILE: {      // The command "delete" allows for explicit deletion of items
                            oLog.info("Executing appendtofile command");
                            executeAppendToFile(tokenizer);
                            break;
                        }
                        default: {
                            oLog.warning("Error\r\n");
                            sendMessage("ERROR\r\n");
                            break;
                        }
                    }
                } else {
                    oLog.warning("CLIENT_ERROR <Empty Input>\r\n");
                    sendMessage("CLIENT_ERROR <Empty Input>\r\n");
                }
            }
        }
    }

    private synchronized void executeWriteToFile(StringTokenizer stringTokenizer) {
        if (stringTokenizer.countTokens() < 2) {
            oLog.warning("CLIENT_ERROR <Data chunk missing>\r\n");
            sendMessage("CLIENT_ERROR <Data chunk missing>\r\n");
            return;
        }

        String fileName = stringTokenizer.nextToken();
        String result = writeToFile(fileName, stringTokenizer);
        parser.noReply = true;
        sendMessage(result);
        sendMessage(END_OF_TRANSMISSION);   //NOTE: Do not remove this
    }

    private synchronized void executeReadFile(StringTokenizer stringTokenizer) {
        if (stringTokenizer.countTokens() < 1) {
            oLog.warning("CLIENT_ERROR <File name missing>\r\n");
            sendMessage("CLIENT_ERROR <File name missing>\r\n");
            return;
        }

        parser.noReply = true;
        String fileName = stringTokenizer.nextToken();
        readAndTransmitFromFile(fileName);
        sendMessage(END_OF_TRANSMISSION);   //NOTE: Do not remove this
    }

    private synchronized void executeAppendToFile(StringTokenizer stringTokenizer) {
        if (stringTokenizer.countTokens() < 2) {
            oLog.warning("CLIENT_ERROR <Key and Value missing>\r\n");
            sendMessage("CLIENT_ERROR <Key and Value missing>\r\n");
            return;
        }

        parser.noReply = true;
        String result = appendToFile(stringTokenizer);
        sendMessage(result);
        sendMessage(END_OF_TRANSMISSION);   //NOTE: Do not remove this
    }

    private synchronized void executeSet(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 6 || stringToken.countTokens() > 7) {
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(SET, stringToken);
        } catch (Exception e) {
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the <value> is not equal to the <bytes>
            oLog.warning("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.containsKey(parser.key)) {
            valuesStore.remove(parser.key);
            valuesStore.put(parser.key, parser.value);
            flagsStore.remove(parser.key);
            flagsStore.put(parser.key, parser.flags);
        } else {
            valuesStore.put(parser.key, parser.value);
            flagsStore.put(parser.key, parser.flags);
        }

        sendMessage("STORED\r\n");
    }

    private synchronized void executeAdd(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 6 || stringToken.countTokens() > 7) {
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(ADD, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the value is not equal to the bytes
            oLog.warning("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.get(parser.key) != null) {
            oLog.warning("NOT_ADDED\r\n");
            sendMessage("NOT_ADDED\r\n");
        } else {
            valuesStore.put(parser.key, parser.value);
            flagsStore.put(parser.key, parser.flags);
            oLog.info("ADDED\r\n");
            sendMessage("ADDED\r\n");
        }
    }

    private synchronized void executeReplace(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 6 || stringToken.countTokens() > 7) { // incorrect no. of arguments
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(REPLACE, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the <value> is not equal to the <bytes>
            oLog.warning("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.get(parser.key) != null) {
            valuesStore.remove(parser.key);
            valuesStore.put(parser.key, parser.value);
            flagsStore.remove(parser.key);
            flagsStore.put(parser.key, parser.flags);
            oLog.info("REPLACED\r\n");
            sendMessage("REPLACED\r\n");
        } else {
            oLog.info("NOT_REPLACED\r\n");
            sendMessage("NOT_REPLACED\r\n");
        }
    }

    private synchronized void executeAppend(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 4 || stringToken.countTokens() > 5) { // incorrect no. of arguments
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(APPEND, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the <value> is not equal to the <bytes>
            oLog.warning("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.get(parser.key) != null) {
            String oldValue = valuesStore.get(parser.key);
            valuesStore.remove(parser.key);
            valuesStore.put(parser.key, oldValue + parser.value);
            oLog.info("APPENDED\r\n");
            sendMessage("APPENDED\r\n");
        } else {
            oLog.info("NOT_APPENDED\r\n");
            sendMessage("NOT_APPENDED\r\n");
        }
    }

    private synchronized void executePrepend(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 4 || stringToken.countTokens() > 5) { // incorrect no. of arguments
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(PREPEND, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the <value> is not equal to the <bytes>
            oLog.warning("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.get(parser.key) != null) {
            String oldValue = valuesStore.get(parser.key);
            valuesStore.remove(parser.key);
            valuesStore.put(parser.key, parser.value + oldValue);
            oLog.info("PREPENDED\r\n");
            sendMessage("PREPENDED\r\n");
        } else {
            oLog.info("NOT_PREPENDED\r\n");
            sendMessage("NOT_PREPENDED\r\n");
        }
    }

    private synchronized void executeCas(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 7 || stringToken.countTokens() > 8) { // incorrect no. of arguments
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(CAS, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if(parser.valueSize == 0)
            return;

        if (parser.valueSize != parser.value.length()) { // Size of the <value> is not equal to the <bytes>
            oLog.info("CLIENT_ERROR <Size & length of value does not match>\r\n");
            sendMessage("CLIENT_ERROR <Size & length of value does not match>\r\n");
            return;
        }

        if (valuesStore.get(parser.key) != null && parser.casKey.equals(flagsStore.get(parser.key))) {
            valuesStore.remove(parser.key);
            valuesStore.put(parser.key, parser.value);
            oLog.info("STORED\r\n");
            sendMessage("STORED\r\n");
        } else if (valuesStore.get(parser.key) == null) {
            valuesStore.put(parser.key, parser.value);
            flagsStore.put(parser.key, parser.flags);
            oLog.info("STORED\r\n");
            sendMessage("STORED\r\n");
        } else {
            oLog.info("NOT STORED\r\n");
            sendMessage("NOT_STORED\r\n");
        }
    }

    private void executeGet(StringTokenizer tokenizer) {
        if (tokenizer.countTokens() == 0) {
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        StringBuilder output = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String key = tokenizer.nextToken();
            if (!tokenizer.hasMoreTokens() && key.endsWith("\\r\\n"))
                key = key.substring(0, key.length() - 4);
            String value = valuesStore.get(key);

            if (value == null)
                output.append("CLIENT_ERROR <key: \"").append(key).append("\" does not exist>\r\n");
            else
                output.append("VALUE ").append(key).append(" ").append(flagsStore.get(key)).append(" ").append(value.length()).append(" \r\n").append(value).append("\r\n");
        }

        output.append("END\r\n");
        oLog.info(output.toString());
        sendMessage(output.toString());
    }

    private synchronized void executeDelete(StringTokenizer stringToken) {
        if (stringToken.countTokens() < 1 || stringToken.countTokens() > 2) { // incorrect no. of arguments
            oLog.warning("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            sendMessage("CLIENT_ERROR <Incorrect No. of Arguments passed>\r\n");
            return;
        }

        try {
            parser.parseCommand(DELETE, stringToken);
        } catch (Exception e) {
            oLog.warning(e.getMessage());
            sendMessage(e.getMessage());
        }

        if (valuesStore.containsKey(parser.key)) {
            valuesStore.remove(parser.key);
            flagsStore.remove(parser.key);
            sendMessage("DELETED\r\n");
            oLog.info("DELETED\r\n");
        } else {
            oLog.info("NOT_FOUND\r\n");
            sendMessage("NOT_FOUND\r\n");
        }
    }

    private synchronized boolean populateMapFromFile(ConcurrentHashMap<String, String> map, String fileName) throws IOException {
        File file = new File(fileName);
        if (file.exists()) {
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(fileName));
            } catch (FileNotFoundException e) {
                oLog.warning("SERVER_ERROR <Error in retrieving data from file>\r\n");
                sendMessage("SERVER_ERROR <Error in retrieving data from file>\r\n");
                return false;
            }

            try {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] str = line.split("\n");
                    for (String s : str) {
                        String[] arr = s.split(",");
                        map.put(arr[0], arr[1]);
                    }
                }
            } catch (Exception e) {
                oLog.warning("SERVER_ERROR <Error in retrieving data from file>\r\n");
                sendMessage("SERVER_ERROR <Error in retrieving data from file>\r\n");
                return false;
            }
        } else {
            file.createNewFile();
        }
        return true;
    }

    private synchronized boolean updateFile(Map<String, String> map, String fileName) {
        try {
            FileWriter fileWriter = new FileWriter(fileName);
            for (String key : map.keySet()) {
                fileWriter.append(key);
                fileWriter.append(",");
                fileWriter.append(map.get(key));
                fileWriter.append("\n");
            }
            fileWriter.close();
        } catch (Exception e) {
            oLog.warning("SERVER_ERROR <Error in storing data to file>\r\n");
            sendMessage("SERVER_ERROR <Error in storing data to file>\r\n");
            return false;
        }
        return true;
    }

    private synchronized String writeToFile(String fileName, StringTokenizer tokenizer) {
        try {
            FileWriter fileWriter = new FileWriter(fileName, false);
            int wordCount = 0;
            while(tokenizer.hasMoreElements()) {
                fileWriter.append(tokenizer.nextToken()).append(" ");
                if(++wordCount == 15) {
                    wordCount = 0;
                    fileWriter.append("\n");
                }
            }
            fileWriter.close();
        } catch (Exception e) {
            oLog.warning("SERVER_ERROR <Error in storing data to file>\r\n");
            return "SERVER_ERROR <Error in storing data to file>\r\n";
        }

        return "WRITE SUCCESSFUL\r\n";
    }

    private synchronized void readAndTransmitFromFile(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            BufferedReader br;
            try {
                br = new BufferedReader(new FileReader(fileName));
            } catch (FileNotFoundException e) {
                oLog.warning("SERVER_ERROR <Error in retrieving data from file>\r\n");
                sendMessage("SERVER_ERROR <Error in retrieving data from file>\r\n");
                return;
            }

            try {
                String line;
                while ((line = br.readLine()) != null) {
                    sendMessage(line + "\n");
                }
                sendMessage(END_OF_TRANSMISSION);
            } catch (Exception e) {
                oLog.warning("SERVER_ERROR <Error in retrieving data from file>\r\n");
                sendMessage("SERVER_ERROR <Error in retrieving data from file>\r\n");
            }
        } else {
            oLog.warning("CLIENT_ERROR <File isn't created in the first place>\r\n");
            sendMessage("CLIENT_ERROR <File isn't created in the first place>\r\n");
        }
    }

    private synchronized String appendToFile(StringTokenizer stringTokenizer) {
        if(stringTokenizer.countTokens() < 3) {
            oLog.warning("CLIENT_ERROR <Key and Value missing>\r\n");
            return "CLIENT_ERROR <Key and Value missing>\r\n";
        }

        String fileName = stringTokenizer.nextToken();

        try {
            FileWriter fileWriter = new FileWriter(fileName, true);
            while(stringTokenizer.hasMoreElements())
                fileWriter.append(stringTokenizer.nextToken()).append(" ");
            fileWriter.append("\n");
            fileWriter.close();
        } catch (Exception e) {
            oLog.warning("SERVER_ERROR <Error in storing data to file>\r\n");
            return "SERVER_ERROR <Error in storing data to file>\r\n";
        }

        oLog.info("WRITE SUCCESSFUL\r\n");
        return "WRITE SUCCESSFUL\r\n";
    }

    private synchronized void sendMessage(String message) {
        try {
            if (parser.noReply)
                out.writeUTF(" ");
            else
                out.writeUTF(message);
            out.flush();
            parser.noReply = false;
        } catch (IOException e) {
            oLog.warning(String.format("Filed during transmitting the message: %s", message) + e.getMessage());
            System.out.println(String.format("Filed during transmitting the message: %s", message) + e.getMessage());
        }
    }

}
