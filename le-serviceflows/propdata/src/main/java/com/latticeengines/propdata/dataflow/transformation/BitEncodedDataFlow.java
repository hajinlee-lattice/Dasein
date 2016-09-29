package com.latticeengines.propdata.dataflow.transformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class BitEncodedDataFlow<C extends TransformationConfiguration, P extends TransformationFlowParameters>
        extends TransformationFlowBase<C, P> {



}
