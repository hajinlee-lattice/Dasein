package com.latticeengines.eai.service.impl.marketo;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.ImportService;

@Component("marketoImportService")
public class MarketoImportServiceImpl implements ImportService {

    @Override
    public void importMetadata() {
    }

    @Override
    public void importData() {
    }

    @Override
    public void init(List<Table> tables) {
    }

    @Override
    public void finalize() {
    }
}
