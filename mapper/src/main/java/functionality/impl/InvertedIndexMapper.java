package functionality.impl;

import functionality.api.Mapper;
import helper.LogHelper;

import java.util.logging.Logger;

public class InvertedIndexMapper implements Mapper {
    private static final Logger oLog = LogHelper.getLogger(InvertedIndexMapper.class.getName());

    @Override
    public String[] generateKeyValuePair(String key, String mapperID) {
        return sanitizeKey(key, mapperID);
    }

    private String[] sanitizeKey(String key, String mapperID) {
        oLog.info("Old value: " + key);
        key = key.replaceAll("[^a-zA-Z0-9]", "");
        key = key.trim();
        oLog.info("Sanitized value: " + key);
        return key.isEmpty() ? null : new String[]{key, "d" + mapperID + " 1"};
    }
}
