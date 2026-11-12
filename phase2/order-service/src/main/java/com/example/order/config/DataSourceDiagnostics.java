package com.example.order.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class DataSourceDiagnostics implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DataSourceDiagnostics.class);
    private final DataSource dataSource;

    public DataSourceDiagnostics(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void run(String... args) {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            String searchPath = "unknown";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW search_path")) {
                if (rs.next()) {
                    searchPath = rs.getString(1);
                }
            } catch (Exception ignored) {}

            log.info("================== DATASOURCE DIAGNOSTICS ==================");
            log.info("Service        : order-service");
            log.info("JDBC URL       : {}", meta.getURL());
            log.info("Database User  : {}", meta.getUserName());
            log.info("Database Name  : {}", conn.getCatalog());
            log.info("Current Schema : {}", conn.getSchema());
            log.info("Search Path    : {}", searchPath);
            log.info("DB Product     : {} {}", meta.getDatabaseProductName(), meta.getDatabaseProductVersion());
            log.info("============================================================");
        } catch (Exception e) {
            log.warn("Could not retrieve datasource metadata: {}", e.getMessage());
        }
    }
}

