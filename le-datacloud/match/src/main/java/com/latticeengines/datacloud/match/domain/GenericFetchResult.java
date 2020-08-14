package com.latticeengines.datacloud.match.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import com.latticeengines.common.exposed.util.KryoUtils;

public class GenericFetchResult {

    private String recordId;

    private Map<String, Object> result;

    public String getRecordId() {
        return recordId;
    }

    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public GenericFetchResult getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, GenericFetchResult.class);
    }
}
