package com.latticeengines.eai.service.impl.file.strategy;

import java.util.Map;
import java.util.Properties;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.file.runtime.mapreduce.CSVExportJob;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.impl.ExportStrategy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("csvFileExportStrategyBase")
public class CSVFileExportStrategyBase extends ExportStrategy {

    private static final Log log = LogFactory.getLog(CSVFileExportStrategyBase.class);

    @Autowired
    private EaiYarnService eaiYarnService;

    public CSVFileExportStrategyBase() {
        this(ExportFormat.CSV);
    }

    protected CSVFileExportStrategyBase(ExportFormat key) {
        super(key);
    }

    @Override
    public void exportData(ProducerTemplate template, Table table, String filter, ExportContext ctx) {
        log.info(String.format("Exporting data for table %s with filter %s", table, filter));
        Properties props = getProperties(ctx, table);

        ApplicationId appId = eaiYarnService.submitMRJob(CSVExportJob.CSV_EXPORT_JOB_TYPE, props);
        ctx.setProperty(ImportProperty.APPID, appId);
    }

    @SuppressWarnings("unused")
    private void updateContextProperties(ExportContext ctx, Table table) {
        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = ctx.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> lastModifiedTimes = ctx.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        processedRecordsMap.put(table.getName(), 0L);
        lastModifiedTimes.put(table.getName(), DateTime.now().getMillis());
    }

    private Properties getProperties(ExportContext ctx, Table table) {

        Properties props = new Properties();
        props.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getEaiQueueNameForSubmission());

        String customer = ctx.getProperty(ExportProperty.CUSTOMER, String.class);
        props.setProperty(MapReduceProperty.CUSTOMER.name(), customer);

        String inputPath = ctx.getProperty(ExportProperty.INPUT_FILE_PATH, String.class);
        props.setProperty(MapReduceProperty.INPUT.name(), inputPath);

        String targetHdfsPath = ctx.getProperty(ExportProperty.TARGETPATH, String.class);
        props.setProperty(MapReduceProperty.OUTPUT.name(), targetHdfsPath);

        props.setProperty("eai.table.schema", JsonUtils.serialize(table));

        props.setProperty("eai.export.displayname",
                ctx.getProperty(ExportProperty.EXPORT_USING_DISPLAYNAME, String.class));
        return props;
    }
}
