package com.latticeengines.propdata.core.testframework;

import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.propdata.core.service.DataSourceService;
import com.latticeengines.propdata.core.service.Database;
import com.latticeengines.propdata.core.service.SQLDialect;

@Component
public class SQLInitializer {

    @Autowired
    @Qualifier(value = "propDataManageJdbcTemplate")
    private JdbcTemplate jdbcTemplateManageDB;

    @Autowired
    private DataSourceService dataSourceService;

    private Log log = LogFactory.getLog(SQLInitializer.class);

    public void initialize() {
        if (SQLDialect.MYSQL.equals(dataSourceService.getSqlDialect(Database.MANAGE))) {
            uploadSourceColumn();
        }
    }

    private void uploadSourceColumn() {
        if (mysqlTableIsEmpty(jdbcTemplateManageDB, "SourceColumn")) {
            truncateMySqlTable(jdbcTemplateManageDB, "SourceColumn");
            uploadTableByTabDelimited(jdbcTemplateManageDB, "SourceColumn");
        }

        if (mysqlTableIsEmpty(jdbcTemplateManageDB, "ExternalColumn") ||
                mysqlTableIsEmpty(jdbcTemplateManageDB, "ColumnMapping")) {
            truncateMySqlTable(jdbcTemplateManageDB, "ExternalColumn");
            uploadTableByTabDelimited(jdbcTemplateManageDB, "ExternalColumn");
            uploadTableByTabDelimited(jdbcTemplateManageDB, "ColumnMapping");
        }
    }

    private boolean isMacOS() {
        return System.getProperty("os.name").toLowerCase().contains("mac");
    }

    private void uploadTableByTabDelimited(JdbcTemplate jdbcTemplate, String tableName) {
        log.info("Uploading " + tableName + " data ...");

        String resource = "sql/" + tableName + ".txt";

        URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
        Assert.assertNotNull(url, "Cannot find " + resource);
        String data = url.getFile();

        String firstLine = readFirstLineOfResource(resource);
        String[] enclosedFields = firstLine.split("\t");
        List<String> fields = new ArrayList<>();
        for (String enclosedField: enclosedFields) {
            fields.add(enclosedField.substring(1, enclosedField.length() - 1));
        }

        String sql =  "LOAD DATA INFILE '" + data + "' INTO TABLE `" + tableName + "` \n" +
                "FIELDS TERMINATED BY '\\t'  ENCLOSED BY ';' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "IGNORE 1 LINES \n" +
                "(" + StringUtils.join(fields, ",") + ");";

        if (isMacOS()) {
            sql = sql.replace("LOAD DATA INFILE", "LOAD DATA LOCAL INFILE");
        }

        jdbcTemplate.execute(sql);
    }

    private void truncateMySqlTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.execute("DELETE FROM `" + tableName + "`;");
    }

    private boolean mysqlTableIsEmpty(JdbcTemplate jdbcTemplate, String tableName) {
        return !(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM `" + tableName + "` LIMIT 1)", Boolean.class));
    }

    private String readFirstLineOfResource(String resource) {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        if (is == null) {
            throw new RuntimeException("Cannot find resource " + resource);
        }
        Scanner scanner = new Scanner(is);
        String line = "";

        if (scanner.hasNextLine()) {
            line = scanner.nextLine();
        }
        scanner.close();

        return line;
    }

}
