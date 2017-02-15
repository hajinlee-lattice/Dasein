package com.latticeengines.eai.service.impl.snowflake;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.eai.service.ExportService;

@Component("snowflakeExportService")
public class SnowflakeExportServiceImpl extends ExportService {

    protected SnowflakeExportServiceImpl() {
        super(ExportDestination.Snowflake);
    }

}
