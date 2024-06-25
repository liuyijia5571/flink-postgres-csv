package com.lyj.util;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String CONFIG_FILE_PREFIX = "application-";
    private static final String CONFIG_FILE_SUFFIX = ".properties";

    public static final String DB_PROFILE = "db_profile";


    public static final String TEST05 = "test05";

    private static PropertiesConfiguration config;

    static {
        // 默认加载开发环境配置
        loadConfiguration("dev");
    }

    static {
        try {
            logger.info("Class.forName(org.postgresql.Driver)");
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load org.postgresql.Driver");
        }
    }

    public static void loadConfiguration(String profile) {
        try {
            logger.info("profile is {}", CONFIG_FILE_PREFIX + profile + CONFIG_FILE_SUFFIX);
            config = new PropertiesConfiguration(CONFIG_FILE_PREFIX + profile + CONFIG_FILE_SUFFIX);
        } catch (ConfigurationException e) {
            logger.error("error message is {}", e.getMessage());
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
