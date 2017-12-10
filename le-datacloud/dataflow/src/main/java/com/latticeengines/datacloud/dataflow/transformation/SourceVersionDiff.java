package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.CheckUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.check.ExceedCntDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.check.ExceedDomDiffBetwenVersionChkParam;
import com.latticeengines.domain.exposed.datacloud.dataflow.AMValidatorParams;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component(SourceVersionDiff.DATAFLOW_BEAN_NAME)
public class SourceVersionDiff extends TransformationFlowBase<BasicTransformationConfiguration, AMValidatorParams> {
    public final static String DATAFLOW_BEAN_NAME = "SourceVersionDiff";
    public final static String TRANSFORMER_NAME = "SourceVersionFlowTransformer";

    public static String getTableName(String source, String version) {
        if (StringUtils.isEmpty(version)) {
            return source;
        }
        return source + "_" + version;
    }

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(AMValidatorParams parameters) {
        Node src = parameters.getBaseTables().size() == 2 ? addSource(parameters.getBaseTables().get(0))
                : addSource(getTableName(parameters.getBaseTables().get(0), parameters.getDiffVersion()));
        Node srcCompared = parameters.getBaseTables().size() == 2 ? addSource(parameters.getBaseTables().get(1))
                : addSource(getTableName(parameters.getBaseTables().get(0), parameters.getDiffVersionCompared()));
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(srcCompared);
        nodeList.add(src);
        ExceedCntDiffBetwenVersionChkParam utilParams1 = new ExceedCntDiffBetwenVersionChkParam();
        utilParams1.setThreshold(parameters.getThreshold());
        Node resultNode1 = CheckUtils.runCheck(nodeList, utilParams1);
        ExceedDomDiffBetwenVersionChkParam utilParams2 = new ExceedDomDiffBetwenVersionChkParam();
        utilParams2.setPrevVersionNotEmptyField(parameters.getCheckNotNullField());
        utilParams2.setCurrVersionNotEmptyField(parameters.getCheckNotNullField());
        utilParams2.setPrevVersionEmptyField(parameters.getCheckNullField());
        utilParams2.setCurrVersionNullField(parameters.getCheckNullField());
        Node resultNode2 = CheckUtils.runCheck(nodeList, utilParams2);
        Node resultNode = resultNode1 //
                .merge(resultNode2);
        return resultNode;
    }

}
