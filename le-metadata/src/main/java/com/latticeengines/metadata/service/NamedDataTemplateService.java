package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface NamedDataTemplateService {

    List<DataUnit> getData(String dataTemplateName, String... namespace);

    long getSchemaCount(String dataTemplateName, String... namespace);

    Flux<ColumnMetadata> getSchema(String dataTemplateName, String... namespace);

    ParallelFlux<ColumnMetadata> getUnorderedSchema(String dataTemplateName, String... namespace);

}
