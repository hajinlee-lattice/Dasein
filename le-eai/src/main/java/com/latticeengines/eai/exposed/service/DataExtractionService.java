package com.latticeengines.eai.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.Table;

public interface DataExtractionService {

    void extractAndImport(List<Table> tables);
}
