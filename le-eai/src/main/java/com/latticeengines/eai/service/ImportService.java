package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.Table;

public interface ImportService {

    /**
     * Import metadata from the specific connector. The original list of table
     * metadata will be decorated with the following:
     * 
     * 1. Attribute length, precision, scale, physical type, logical type 2.
     * Avro schema associated with the Table
     * 
     * @param tables
     *            list of tables that only has table name and attribute names
     * @return
     */
    List<Table> importMetadata(List<Table> tables);

    void importData(List<Table> tables);

}
