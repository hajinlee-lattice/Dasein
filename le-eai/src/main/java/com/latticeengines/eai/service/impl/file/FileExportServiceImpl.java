package com.latticeengines.eai.service.impl.file;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.BaseProperty;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.eai.service.impl.ExportStrategy;

@Component("fileExportService")
public class FileExportServiceImpl extends ExportService {

    protected FileExportServiceImpl() {
        super(ExportDestination.FILE);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        ExportStrategy strategy = ExportStrategy.getExportStrategy(exportConfig.getExportFormat());
        Table table = exportConfig.getTable();
        configureExportContext(exportConfig, context);
        strategy.exportData(null, table, null, context);
    }

    public ExportContext configureExportContext(ExportConfiguration exportConfig, ExportContext context) {
        context.setProperty(ExportProperty.TARGETPATH, exportConfig.getExportTargetPath());
        context.setProperty(ExportProperty.CUSTOMER, exportConfig.getCustomerSpace().toString());

        Table table = exportConfig.getTable();

        String inputPath = exportConfig.getExportInputPath();
        if (StringUtils.isNotEmpty(inputPath)) {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, inputPath);
        } else {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, ExtractUtils
                    .getSingleExtractPath(context.getProperty(BaseProperty.HADOOPCONFIG, Configuration.class), table));
        }
        boolean exportUsingDisplayName = exportConfig.getUsingDisplayName();
        context.setProperty(ExportProperty.EXPORT_USING_DISPLAYNAME, String.valueOf(exportUsingDisplayName));
        context.setProperty(ExportProperty.EXPORT_EXCLUSION_COLUMNS, exportConfig.getExclusionColumns());
        context.setProperty(ExportProperty.EXPORT_INCLUSION_COLUMNS, exportConfig.getInclusionColumns());
        return context;
    }

}
