package com.latticeengines.eai.service.impl.redshift;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.eai.service.ExportService;

@Component("redshiftExportService")
public class RedshiftExportServiceImpl extends ExportService {

    protected RedshiftExportServiceImpl() {
        super(ExportDestination.REDSHIFT);
    }

}
