package com.latticeengines.eai.service.impl.eloqua;

import java.util.List;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ImportService;

@Component("eloquaImportService")
public class EloquaImportServiceImpl extends ImportService {

    public EloquaImportServiceImpl() {
        super(SourceType.ELOQUA);
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration extractionConfig, ImportContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext context) {
        // TODO Auto-generated method stub

    }

}
