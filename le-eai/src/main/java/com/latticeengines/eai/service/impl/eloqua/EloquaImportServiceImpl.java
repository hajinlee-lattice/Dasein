package com.latticeengines.eai.service.impl.eloqua;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.DataExtractionConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.ImportService;

@Component("eloquaImportService")
public class EloquaImportServiceImpl extends ImportService {

    @Override
    public List<Table> importMetadata(DataExtractionConfiguration extractionConfig, ImportContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void importDataAndWriteToHdfs(DataExtractionConfiguration extractionConfig, ImportContext context) {
        // TODO Auto-generated method stub

    }

}
