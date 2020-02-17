package com.latticeengines.objectapi.util;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;

public final class TempListUtils {

    protected TempListUtils() {
        throw new UnsupportedOperationException();
    }

    public static String newTempTableName() {
        return NamingUtils.timestamp(NamingUtils.randomSuffix("templist_", 8)).toLowerCase();
    }

    public static String getCheckSum(ConcreteRestriction restriction) {
        CollectionLookup collectionLookup = (CollectionLookup) restriction.getRhs();
        Class<?> fieldClz = TempListUtils.getFieldClz(collectionLookup.getValues());
        AttributeLookup attributeLookup = (AttributeLookup) restriction.getLhs();
        String attrName = attributeLookup.getAttribute();
        List<List<Object>> vals = insertVals(fieldClz, collectionLookup.getValues());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, vals);
        String content = attrName + ":" + new String(bos.toByteArray());
        return HashUtils.getMD5CheckSum(content);
    }

    public static Class<?> getFieldClz(Collection<Object> vals) {
        Class<?> fieldClz = null;
        for (Object val: vals) {
            if (val != null) {
                Class<?> valClz = val.getClass();
                if (fieldClz == null) {
                    fieldClz = valClz;
                } else if (!valClz.equals(fieldClz)) {
                    fieldClz = String.class;
                }
            }
            if (String.class.equals(fieldClz)) {
                // no need to scan further
                break;
            }
        }
        if (fieldClz == null) {
            fieldClz = String.class;
        }
        return fieldClz;
    }

    public static List<List<Object>> insertVals(Class<?> fieldClz, Collection<Object> vals) {
        List<List<Object>> lst = new ArrayList<>();
        vals.forEach(val -> {
            if (val == null || fieldClz.equals(val.getClass())) {
                lst.add(Collections.singletonList(val));
            } else if (String.class.equals(fieldClz)) {
                lst.add(Collections.singletonList(String.valueOf(val)));
            } else {
                throw new IllegalArgumentException("Cannot insert "+ val + " as " + fieldClz);
            }
        });
        return lst;
    }

}
