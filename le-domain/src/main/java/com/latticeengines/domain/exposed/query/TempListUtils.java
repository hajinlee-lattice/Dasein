package com.latticeengines.domain.exposed.query;

import java.io.ByteArrayOutputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.latticeengines.common.exposed.util.HashUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.common.exposed.util.NamingUtils;

public final class TempListUtils {

    protected TempListUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String TEMPLIST_PREFIX = "templist_";
    private static final Pattern PATTERN = Pattern.compile(String.format("%s[A-Za-z0-9]+_(?<date>[A-Za-z0-9_]{%d})",
            TEMPLIST_PREFIX, "yyyy_MM_dd_HH_mm_ss_zzz".length()));
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss_z");

    public static String newTempTableName() {
        return NamingUtils.timestamp(NamingUtils.randomSuffix(TEMPLIST_PREFIX, 8)).toLowerCase();
    }

    public static LocalDate parseDateFromTableName(String tableName) {
        Matcher matcher = PATTERN.matcher(tableName);
        if (matcher.matches()) {
            String dateStr = matcher.group("date").toUpperCase();
            return LocalDate.parse(dateStr, FORMATTER);
        } else {
            return null;
        }
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
