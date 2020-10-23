package server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public final class Server {
    private static final int LISTENING_PORT = 10254;
    protected static final String VALUES_CSV = "values.csv";
    protected static final String FLAGS_CSV = "flags.csv";

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(LISTENING_PORT);
            System.out.println("Server started");

            while (true) {
                Socket socket = serverSocket.accept();
                CommandExecutor commandExecutor = new CommandExecutor(socket);
                commandExecutor.start();
            }
        } catch (IOException ex) {
            System.out.println(Arrays.toString(ex.getStackTrace()));
        }
    }
}