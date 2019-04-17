package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;
import java.util.stream.Collectors;

import org.codehaus.plexus.util.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.BitEncodeUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.EncodedBitOperationFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TechIndicatorsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(HGDataTechIndicatorsFlow.DATAFLOW_BEAN_NAME)
public class HGDataTechIndicatorsFlow
 extends ConfigurableFlowBase<TechIndicatorsConfig> {

    public static final String DATAFLOW_BEAN_NAME = "hgDataTechIndicatorsFlow";
    public static final String TRANSFORMER_NAME = "hgDataTechIndicatorsTransformer";
    private static final String SEGMENT_INDICATORS = "SegmentTechIndicators";
    private static final String SUPPLIER_INDICATORS = "SupplierTechIndicators";

    private TechIndicatorsConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node hgClean = addSource(parameters.getBaseTables().get(0));
        Node hgTechInd = addSource(parameters.getBaseTables().get(1));
        List<SourceColumn> sourceColumns = parameters.getColumns();
        List<SourceColumn> nonDepreCols = getNonDeprecatedCols(sourceColumns);
        List<SourceColumn> depreCols = getDeprecatedCols(sourceColumns);
        Node nonDepreEncoded = BitEncodeUtils.encode(hgClean, config.getGroupByFields(), nonDepreCols)
                .renamePipe("NonDepreEncoded");
        Node depreEncoded = BitEncodeUtils.decodeAndEncode(hgTechInd, config.getGroupByFields(), depreCols).renamePipe("DepreEncoded");
        Node combined = combine(nonDepreEncoded, depreEncoded);
        return combined.addTimestamp(config.getTimestampField());
    }

    private Node combine(Node nonDepreEncoded, Node depreEncoded) {
        String[] encodedAttrs = { SEGMENT_INDICATORS, SUPPLIER_INDICATORS };
        for (String attr : encodedAttrs) {
            nonDepreEncoded = nonDepreEncoded.rename(new FieldList(attr), new FieldList("_NONDEPRE_" + attr));
            nonDepreEncoded = nonDepreEncoded.retain(new FieldList(nonDepreEncoded.getFieldNames()));
            depreEncoded = depreEncoded.rename(new FieldList(attr), new FieldList("_DEPRE_" + attr));
            depreEncoded = depreEncoded.retain(new FieldList(depreEncoded.getFieldNames()));
        }
        Node combined = nonDepreEncoded.join(new FieldList(config.getGroupByFields()), depreEncoded,
                new FieldList(config.getGroupByFields()), JoinType.LEFT);
        combined = combined.retain(new FieldList(combined.getFieldNames()));
        for (String attr : encodedAttrs) {
            combined = combined.apply(
                    new EncodedBitOperationFunction(attr, "_NONDEPRE_" + attr, "_DEPRE_" + attr,
                            EncodedBitOperationFunction.Operation.OR),
                    new FieldList(combined.getFieldNames()), new FieldMetadata(attr, String.class))
                    .discard("_NONDEPRE_" + attr, "_DEPRE_" + attr);
        }
        return combined;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TechIndicatorsConfig.class;
    }

    private List<SourceColumn> getNonDeprecatedCols(List<SourceColumn> sourceColumns) {
        return sourceColumns.stream().filter(sourceColumn -> StringUtils.isNotEmpty(sourceColumn.getArguments())
                && !sourceColumn.getArguments().contains("DecodeStrategy"))
                .collect(Collectors.toList());
    }

    private List<SourceColumn> getDeprecatedCols(List<SourceColumn> sourceColumns) {
        return sourceColumns.stream().filter(sourceColumn -> StringUtils.isNotEmpty(sourceColumn.getArguments())
                && sourceColumn.getArguments().contains("DecodeStrategy"))
                .collect(Collectors.toList());
    }

}
