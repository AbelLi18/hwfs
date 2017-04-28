package com.abel.hwfs.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class PropertiesUtil {

    private static Properties properties = null;

    static{
        InputStream in = null;
        try {
            in = PropertiesUtil.class.getClassLoader().getResourceAsStream("conf.properties");
            properties = new Properties();
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
