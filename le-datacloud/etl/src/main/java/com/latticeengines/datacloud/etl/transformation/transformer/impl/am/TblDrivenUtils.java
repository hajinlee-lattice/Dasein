package com.latticeengines.datacloud.etl.transformation.transformer.impl.am;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.spark.am.TblDrivenTxfmrConfig;

@Component("tblDrivenUtils")
public class TblDrivenUtils<T extends TblDrivenTxfmrConfig, E extends TblDrivenFuncConfig> {

    private static final Logger log = LoggerFactory.getLogger(TblDrivenUtils.class);

    @Inject
    SourceAttributeEntityMgr attrMgr;

    public List<SourceAttribute> getAttributes(T config, String transformerName) {
        return attrMgr.getAttributes(config.getSource(), config.getStage(), transformerName);
    }

    @SuppressWarnings("unchecked")
    public List<E> getAttributeFuncs(T config, List<SourceAttribute> srcAttrs,
            Class<? extends TblDrivenFuncConfig> funcClazz) {
        List<E> funcList = new ArrayList<E>();
        boolean invalidConf = false;
        for (SourceAttribute attr : srcAttrs) {
            String attrName = attr.getAttribute();
            String confJson = attr.getArguments();
            E funcConfig = null;
            try {
                funcConfig = (E) JsonUtils.deserialize(confJson, funcClazz);
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

}
