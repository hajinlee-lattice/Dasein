package com.latticeengines.db.exposed.schemagen;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

public class EnableSQLServerSnapshotIsolation {

    public static void main(String[] args) throws Exception {
        String dbPropertiesFilepath = args[0];
        System.setProperty("DATAPLATFORM_PROPDIR", dbPropertiesFilepath);

        @SuppressWarnings("resource")
        ApplicationContext context = new ClassPathXmlApplicationContext("test-dataplatform-context.xml");

        JdbcTemplate ledpJdbcTemplate = (JdbcTemplate) context.getBean("ledpJdbcTemplate");
        String catalog = ledpJdbcTemplate.getDataSource().getConnection().getCatalog();

        ledpJdbcTemplate.execute(String.format("ALTER DATABASE %s SET allow_snapshot_isolation ON", catalog));
        ledpJdbcTemplate.execute(String.format("ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE", catalog));
        ledpJdbcTemplate.execute(String.format("ALTER DATABASE %s SET read_committed_snapshot ON", catalog));
        ledpJdbcTemplate.execute(String.format("ALTER DATABASE %s SET MULTI_USER", catalog));
    }

}
