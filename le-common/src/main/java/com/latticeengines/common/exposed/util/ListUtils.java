package com.latticeengines.common.exposed.util;

import java.util.List;

import com.google.api.client.util.Lists;

public class ListUtils {
    public static List<String> trim(List<String> in) {
        List<String> out = Lists.newArrayList(in);
        for (int i = 0; i < in.size(); ++i) {
            in.set(i, in.get(i).trim());
        }
        return out;
    }
}
