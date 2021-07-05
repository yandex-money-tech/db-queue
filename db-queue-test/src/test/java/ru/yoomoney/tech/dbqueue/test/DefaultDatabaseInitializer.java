package ru.yoomoney.tech.dbqueue.test;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.util.Optional;

/**
 * @author Oleg Kandaurov
 * @since 11.06.2021
 */
public class DefaultDatabaseInitializer {

    public static final String DEFAULT_TABLE_NAME = "queue_default";

    public static final String PG_DEFAULT_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";

    public static final String PG_TRACING_TABLE_DDL = "CREATE TABLE %s (\n" +
            "  id                BIGSERIAL PRIMARY KEY,\n" +
            "  queue_name        TEXT NOT NULL,\n" +
            "  payload           TEXT,\n" +
            "  created_at        TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  next_process_at   TIMESTAMP WITH TIME ZONE DEFAULT now(),\n" +
            "  attempt           INTEGER                  DEFAULT 0,\n" +
            "  reenqueue_attempt INTEGER                  DEFAULT 0,\n" +
            "  total_attempt     INTEGER                  DEFAULT 0,\n" +
            "  trace_info        TEXT\n" +
            ");" +
            "CREATE INDEX %s_name_time_desc_idx\n" +
            "  ON %s (queue_name, next_process_at, id DESC);\n" +
            "\n";


    private static JdbcTemplate pgJdbcTemplate;
    private static TransactionTemplate pgTransactionTemplate;

    public static synchronized void initialize() {
        if (pgJdbcTemplate != null) {
            return;
        }

        String ryukImage = Optional.ofNullable(System.getProperty("testcontainers.ryuk.container.image"))
                .orElse("quay.io/testcontainers/ryuk:0.2.3");
        TestcontainersConfiguration.getInstance()
                .updateGlobalConfig("ryuk.container.image", ryukImage);

        String postgresImage = Optional.ofNullable(System.getProperty("testcontainers.postgresql.container.image"))
                .orElse("postgres:9.5");
        PostgreSQLContainer<?> dbContainer = new PostgreSQLContainer<>(postgresImage);
        dbContainer.withEnv("POSTGRES_INITDB_ARGS", "--nosync");
        dbContainer.withCommand("postgres -c fsync=off -c full_page_writes=off -c synchronous_commit=off");
        dbContainer.start();
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(dbContainer.getJdbcUrl());
        dataSource.setPassword(dbContainer.getPassword());
        dataSource.setUser(dbContainer.getUsername());
        pgJdbcTemplate = new JdbcTemplate(dataSource);
        pgTransactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        pgTransactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        pgTransactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);

        createTable(PG_DEFAULT_TABLE_DDL, DEFAULT_TABLE_NAME);
    }

    public static void createTable(String ddlTemplate, String tableName) {
        initialize();
        executeDdl(String.format(ddlTemplate, tableName, tableName, tableName));
    }

    private static void executeDdl(String ddl) {
        initialize();
        getTransactionTemplate().execute(status -> {
            getJdbcTemplate().execute(ddl);
            return new Object();
        });
    }

    public static JdbcTemplate getJdbcTemplate() {
        initialize();
        return pgJdbcTemplate;
    }

    public static TransactionTemplate getTransactionTemplate() {
        initialize();
        return pgTransactionTemplate;
    }
}
