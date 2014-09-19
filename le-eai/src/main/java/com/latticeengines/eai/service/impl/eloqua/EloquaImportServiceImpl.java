package com.latticeengines.eai.service.impl.eloqua;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.service.ImportService;

@Component("eloquaImportService")
public class EloquaImportServiceImpl implements ImportService {

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
