package com.latticeengines.domain.exposed.datacloud.match;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.latticeengines.common.exposed.util.KryoUtils;

public class PrimeAccount {

    public static final String DunsNumber = "duns_number";
    public static final String ENRICH_ERROR_CODE = "enrich_error_code";

    private String id;
    private Map<String, Object> result;

    private PrimeAccount() {
    }

    public PrimeAccount(String id, Map<String, Object> result) {
        this.id = id;
        if (MapUtils.isEmpty(result)) {
            this.result = new HashMap<>();
        } else {
            this.result = new HashMap<>(result);
        }
    }

    public String getId() {
        return id;
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public PrimeAccount getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, PrimeAccount.class);
    }
}
