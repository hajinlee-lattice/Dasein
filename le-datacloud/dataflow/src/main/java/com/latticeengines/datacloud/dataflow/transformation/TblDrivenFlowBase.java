package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenTransformerConfig;

public abstract class TblDrivenFlowBase<T extends TblDrivenTransformerConfig, E extends TblDrivenFuncConfig>
       extends ConfigurableFlowBase<T> {

    private static final Logger log = LoggerFactory.getLogger(TblDrivenFlowBase.class);

    @Inject
    SourceAttributeEntityMgr attrMgr;

    protected List<SourceAttribute> getAttributes(T config) {
        return attrMgr.getAttributes(config.getSource(), config.getStage(), getTransformerName());
    }

    @SuppressWarnings("unchecked")
    protected List<E> getAttributeFuncs(T config) {
        List<E> funcList = new ArrayList<E>();
        List<SourceAttribute> srcAttrs = getAttributes(config);

        boolean invalidConf = false;
        for (SourceAttribute attr : srcAttrs) {
            String attrName = attr.getAttribute();
            String confJson = attr.getArguments();
            E funcConfig = null;
            try {
                funcConfig = (E)JsonUtils.deserialize(confJson, getTblDrivenFuncConfigClass());
                funcConfig.setTarget(attrName);
            } catch (Exception e) {
                log.error("Function config for Attribute " + attrName + "is invalid", e);
                config = null;
                invalidConf = true;
            }
            if (!invalidConf) {
                funcList.add(funcConfig);
            }
        }

        return (invalidConf ? null : funcList);
    }

    protected Map<String, Node> initiateSourceMap(TransformationFlowParameters parameters, TblDrivenTransformerConfig config) {

        List<String> tableNames = parameters.getBaseTables();
        List<String> sourceNames = config.getTemplates();

        if (tableNames.size() != sourceNames.size()) {
            log.error("Source names and tables does not match");
            return null;
        }

        Map<String, Node> sourceMap = new HashMap<String, Node>();

        for (int i = 0; i < tableNames.size(); i++) {
             String tblName = tableNames.get(i);
            log.info("Adding source : " + tblName + " as " + sourceNames.get(i));
             sourceMap.put(sourceNames.get(i), addSource(tblName));
        }

        return sourceMap;
    }

    public abstract Class<? extends TblDrivenFuncConfig> getTblDrivenFuncConfigClass();
}
