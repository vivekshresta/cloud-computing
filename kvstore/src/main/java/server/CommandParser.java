package server;


import helper.LogHelper;

import java.util.StringTokenizer;
import java.util.logging.Logger;

public class CommandParser {
    private static final Logger oLog = LogHelper.getLogger(CommandParser.class.getName());
    protected String key;
    protected String value;
    protected String flags;
    protected String expTime;
    protected int valueSize;
    protected String casKey;
    protected boolean noReply;

    public synchronized void parseCommand(String commandType, StringTokenizer stringToken) throws Exception {
        switch (commandType) {
            case CommandExecutor.SET:
            case CommandExecutor.ADD:
            case CommandExecutor.REPLACE: {
                key = stringToken.nextToken();
                flags = stringToken.nextToken();
                expTime = stringToken.nextToken();
                try {
                    valueSize = Integer.parseInt(stringToken.nextToken());
                } catch (Exception e) {
                    oLog.warning("CLIENT_ERROR <Size is not Integer>\r\n");
                    throw new Exception("CLIENT_ERROR <Size is not Integer>\r\n");
                }
                readValueFromCommand(stringToken);
                break;
            }
            // these does not have flags
            case CommandExecutor.APPEND:
            case CommandExecutor.PREPEND: {
                key = stringToken.nextToken();
                valueSize = Integer.parseInt(stringToken.nextToken());
                readValueFromCommand(stringToken);
                break;
            } case CommandExecutor.CAS: {
                key = stringToken.nextToken();
                flags = stringToken.nextToken();
                expTime = stringToken.nextToken();
                try {
                    valueSize = Integer.parseInt(stringToken.nextToken());
                } catch (Exception e) {
                    oLog.warning("CLIENT_ERROR <Size is not Integer>\r\n");
                    throw new Exception("CLIENT_ERROR <Size is not Integer>\r\n");
                }
                casKey = stringToken.nextToken();
                readValueFromCommand(stringToken);
                break;
            } case CommandExecutor.DELETE: {
                key = stringToken.nextToken();
                if (stringToken.hasMoreTokens()) {
                    if (stringToken.nextToken().equals(CommandExecutor.NOREPLY))
                        noReply = true;
                } else if (key.endsWith("\\r\\n")) {
                    key = key.substring(0, key.length() - 4);
                }
                break;
            }
        }

    }

    private synchronized void readValueFromCommand(StringTokenizer stringToken) {
        String token = stringToken.nextToken();
        value = null;
        if (token.equals(CommandExecutor.NOREPLY)) {
            noReply = true;
            value = stringToken.nextToken();
            if (value.startsWith("\\r\\n"))
                value = value.substring(4);
        } else if (token.startsWith("\\r\\n")) {
            value = token.substring(4);
        }
    }
}
