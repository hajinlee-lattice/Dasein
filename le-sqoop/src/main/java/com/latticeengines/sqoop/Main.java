package com.latticeengines.sqoop;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

public final class Main {

    protected Main() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static SqoopJobService sqoopJobService;

    public static void main(String[] args) throws Exception {
        ClassPathXmlApplicationContext appContext = //
                new ClassPathXmlApplicationContext("sqoop-cli-context.xml");
        sqoopJobService = appContext.getBean("sqoopJobService", SqoopJobService.class);
        exportToAurora(new YarnConfiguration());
    }

    private static void exportToAurora(Configuration yarnConfiguration) throws IOException {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("files/SKU_Mfg_Dell.txt");
        String targetTable = "STG_SKU_MANUFACTURER";
        String sourceDir = "/tmp/SKU_Mfg_Dell.txt";
        List<String> targetColumns = Arrays.asList("MFG_NAME", "SUBCLASS_DESC", "ITM_SHRT_DESC", "MFG_PART_NUM");

        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, sourceDir);

        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setTable(targetTable) //
                .setSourceDir(sourceDir) //
                .setDbCreds(getAuroraServerCreds()) //
                .setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .setCustomer("DellEbi_SKU_Mfg") //
                .setNumMappers(1) //
                .setExportColumns(targetColumns) //
                .build();

        ApplicationId appId = sqoopJobService.exportData(exporter, yarnConfiguration);
        log.info(String.format("Waiting for appId %s", appId));
        @SuppressWarnings("deprecation")
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        log.info("Final Status: " + status);
    }

    private static DbCreds getAuroraServerCreds() {
        // FIXME: (YSong-M24) to be changed to dellebi properties file later
        String dbUrl = "jdbc:mysql://lpi-dev-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com/DellEBI?autoReconnect=true&useSSL=false&user=$$USER$$&password=$$PASSWD$$";
        String dbUser = "LPI";
        String dbPassword = CipherUtils.decrypt("bi0mpJJNxiYpEka5C6JO4o75qVXoc80R7ma84i2eK5nKGejJiA0QY8p8RzlrlKU7");
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(dbUrl)
                .user(dbUser)
                .clearTextPassword(dbPassword)
                .dbType("MySQL");
        return new DbCreds(builder);
    }

}
