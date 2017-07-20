package com.latticeengines.playmaker.service;

import java.util.List;
import java.util.Map;

public interface LpiPMPlay {

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds);

    int getPlayCount(long start, List<Integer> playgroupIds);
}
