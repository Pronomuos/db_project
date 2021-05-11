package com.itmo.java.basics.config;

public class DatabaseConfig {
    public static final String DEFAULT_WORKING_PATH = "db_files";
    final private String workingPath;

    public DatabaseConfig(String workingPath) {
        this.workingPath = workingPath.equals("") ?
                System.getProperty("user.dir").concat('/' + DEFAULT_WORKING_PATH) : workingPath;
    }
    public DatabaseConfig() {
        this.workingPath = System.getProperty("user.dir").concat('/' + DEFAULT_WORKING_PATH);
    }

    public String getWorkingPath() {
        return workingPath;
    }
}
