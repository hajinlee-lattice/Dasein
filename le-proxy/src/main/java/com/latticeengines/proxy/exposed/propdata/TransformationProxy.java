package com.latticeengines.proxy.exposed.propdata;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.transformation.TransformationRequest;
import com.latticeengines.network.exposed.propdata.TransformationInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("transformationProxy")
public class TransformationProxy extends BaseRestApiProxy implements TransformationInterface {

    public TransformationProxy() {
        super("propdata/transformations");
    }

    @Override
    public List<TransformationProgress> scan(String hdfsPod) {
        String url = constructUrl("/?podid={hdfsPod}", hdfsPod);
        List<?> list = post("scan_transformation", url, "", List.class);
        List<TransformationProgress> progresses = new ArrayList<>();
        for (Object obj : list) {
            String json = JsonUtils.serialize(obj);
            TransformationProgress progress = JsonUtils.deserialize(json, TransformationProgress.class);
            progresses.add(progress);
        }
        return progresses;
    }

    @Override
    public TransformationProgress transform(TransformationRequest transformationRequest, String hdfsPod) {
        hdfsPod = StringUtils.isEmpty(hdfsPod) ? "" : hdfsPod;
        String url = constructUrl("/internal?podid={hdfsPod}", hdfsPod);
        return post("transform", url, transformationRequest, TransformationProgress.class);
    }

}
