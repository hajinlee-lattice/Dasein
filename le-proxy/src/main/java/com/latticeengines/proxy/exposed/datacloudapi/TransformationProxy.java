package com.latticeengines.proxy.exposed.datacloudapi;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;
import com.latticeengines.network.exposed.propdata.TransformationInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("transformationProxy")
public class TransformationProxy extends MicroserviceRestApiProxy implements TransformationInterface {

    public TransformationProxy() {
        super("datacloudapi/transformations");
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

    @Override
    public TransformationProgress transform(PipelineTransformationRequest transformationRequest, String hdfsPod) {
        hdfsPod = StringUtils.isEmpty(hdfsPod) ? "" : hdfsPod;
        String url = constructUrl("/pipeline?podid={hdfsPod}", hdfsPod);
        return post("transform", url, transformationRequest, TransformationProgress.class);
    }

    @Override
    public TransformationProgress getProgress(String rootOperationUid) {
        String url = constructUrl("/progress?rootOperationUid={rootOperationUid}", rootOperationUid);
        return get("getProgress", url, TransformationProgress.class);
    }
}
