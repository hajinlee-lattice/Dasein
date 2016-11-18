package com.latticeengines.datacloud.etl.transformation.service;

import java.util.List;

import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;

public interface TransformerService {

    Transformer findTransformerByName(String sourceName);

    List<Transformer> getTransformers();

}
