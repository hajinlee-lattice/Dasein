package com.latticeengines.datacloud.etl.service;

import java.util.List;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.domain.exposed.camille.CustomerSpace;


public interface SourceService {

    Source findBySourceName(String sourceName);

    List<Source> getSources();

    Source createSource(String sourceName);

    /**
     * This is to create an shell table with only extra path and table name
     * hdfs source entity mgr has a method to materialize the table with more details,
     * using the generated avro
     */
    TableSource createTableSource(String tableNamePrefix, String version, CustomerSpace customerSpace);

    Source findOrCreateSource(String sourceName);

    boolean deleteSource(Source source);
}
