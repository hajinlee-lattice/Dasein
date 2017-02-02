package com.latticeengines.eai.service.impl.snowflake;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.ExportService;

@Component("snowflakeExportService")
public class SnowflakeExportServiceImpl extends ExportService {

    @Autowired
    private EaiYarnService eaiYarnService;

    protected SnowflakeExportServiceImpl() {
        super(ExportDestination.Snowflake);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        ApplicationId applicationId = eaiYarnService.submitSingleYarnContainer(exportConfig);
        context.setProperty(ExportProperty.APPID, applicationId);
    }

}
