package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public interface GenericMetadataService {

    ColumnSelection parseColumnSelection(MatchInput input);

    List<ColumnMetadata> getOutputSchema(MatchInput input, ColumnSelection columnSelection);

    Schema getOutputAvroSchema(MatchInput input, ColumnSelection columnSelection);

}
