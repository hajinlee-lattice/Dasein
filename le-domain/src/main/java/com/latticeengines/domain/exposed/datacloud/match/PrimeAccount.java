package com.latticeengines.domain.exposed.datacloud.match;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.latticeengines.common.exposed.util.KryoUtils;

public class PrimeAccount {

    public static final String DunsNumber = "duns_number";

    private Map<String, Object> result;

    private PrimeAccount() {
    }

    public PrimeAccount(Map<String, Object> result) {
        if (MapUtils.isEmpty(result)) {
            this.result = new HashMap<>();
        } else {
            this.result = new HashMap<>(result);
        }
    }

    public String getId() {
        return (String) result.get(DunsNumber);
    }

    public Map<String, Object> getResult() {
        return result;
    }

    public void setResult(Map<String, Object> result) {
        this.result = result;
    }

    public PrimeAccount getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, PrimeAccount.class);
    }
}
