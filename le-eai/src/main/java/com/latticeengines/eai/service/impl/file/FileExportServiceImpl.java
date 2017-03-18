package com.latticeengines.eai.service.impl.file;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    @Autowired
    private Configuration yarnConfiguration;

    protected FileExportServiceImpl() {
        super(ExportDestination.FILE);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        context.setProperty(ExportProperty.TARGETPATH, exportConfig.getExportTargetPath());
        context.setProperty(ExportProperty.CUSTOMER, exportConfig.getCustomerSpace().toString());
        ExportStrategy strategy = ExportStrategy.getExportStrategy(exportConfig.getExportFormat());
        Table table = exportConfig.getTable();

        String inputPath = exportConfig.getExportInputPath();
        if (StringUtils.isNotEmpty(inputPath)) {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, inputPath);
        } else {
            context.setProperty(ExportProperty.INPUT_FILE_PATH,
                    ExtractUtils.getSingleExtractPath(yarnConfiguration, table));
        }
        boolean exportUsingDisplayName = exportConfig.getUsingDisplayName();
        context.setProperty(ExportProperty.EXPORT_USING_DISPLAYNAME, String.valueOf(exportUsingDisplayName));
        strategy.exportData(null, table, null, context);
    }

}
