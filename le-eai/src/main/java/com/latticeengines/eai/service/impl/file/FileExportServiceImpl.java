package com.latticeengines.eai.service.impl.file;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.eai.service.impl.ExportStrategy;

@Component("fileExportService")
public class FileExportServiceImpl extends ExportService {

    @Autowired
    private ExportContext exportContext;

    protected FileExportServiceImpl() {
        super(ExportDestination.FILE);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        context.setProperty(ExportProperty.HDFSFILE, //
                exportConfig.getProperties().get(ExportProperty.HDFSFILE));
        ExportStrategy strategy = ExportStrategy.getExportStrategy(exportConfig.getExportFormat());
        List<Table> tables = exportConfig.getTables();
        for (Table table : tables) {
            strategy.exportData(null, table, null, context);
        }
    }

    @Override
    public ApplicationId submitDataExportJob(ExportConfiguration exportConfig) {
        exportDataFromHdfs(exportConfig, exportContext);
        return exportContext.getProperty(ImportProperty.APPID, ApplicationId.class);
    }

}
