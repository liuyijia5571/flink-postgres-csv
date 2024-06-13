package com.example.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ConfigLoader {
    private static final String CONFIG_FILE_PREFIX = "application-";
    private static final String CONFIG_FILE_SUFFIX = ".properties";

    public static final String TEST05 = "test05";

    private static PropertiesConfiguration config;

    static {
        // 默认加载开发环境配置
        loadConfiguration("dev");
    }

    public static void loadConfiguration(String profile) {
        try {
            config = new PropertiesConfiguration(CONFIG_FILE_PREFIX + profile + CONFIG_FILE_SUFFIX);
        } catch (ConfigurationException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load configuration file " + CONFIG_FILE_PREFIX + profile + CONFIG_FILE_SUFFIX);
        }
    }

    public static String getDatabaseUrl() {
        return config.getString("database.url");
    }

    public static String getDatabaseUsername() {
        return config.getString("database.username");
    }

    public static String getDatabasePassword() {
        return config.getString("database.password");
    }
}
