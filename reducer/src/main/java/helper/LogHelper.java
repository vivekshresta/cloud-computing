package helper;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class LogHelper {

    public static Logger getLogger(String className) {
        Logger logger = Logger.getLogger(className);
        FileHandler fh = null;
        try {
            String usingSystemProperty = System.getProperty("user.dir");
            fh = new FileHandler(usingSystemProperty + "/reducer/src/main/resources/logs/ReducerLog.log");
        } catch (Exception e) {
            e.printStackTrace();
        }

        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);

        return logger;
    }
}