package com.latticeengines.dataflow.runtime.cascading.propdata.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MultiListCrossProductUtil implements Serializable {

    private static final long serialVersionUID = 864056936544275118L;

    /*
     * this util will make sure that first entry in the result contains first
     * elements from all the lists, this allows us to easily disregard lowest
     * most dimension combination in stats rollup logic
     */
    public List<List<Long>> calculateCross(List<List<Long>> inputLists) {
        checkForDuplicates(inputLists);

        return cross(inputLists);
    }

    private List<List<Long>> cross(List<List<Long>> inputLists) {
        List<List<Long>> cross = new ArrayList<>();

        if (inputLists.size() == 1) {
            for (Long val : inputLists.get(0)) {
                List<Long> resList = new ArrayList<>();
                resList.add(val);
                cross.add(resList);
            }
            return cross;
        }

        List<List<Long>> partialInputLists = new ArrayList<>();
        List<Long> firstList = inputLists.get(0);
        for (int i = 1; i < inputLists.size(); i++) {
            partialInputLists.add(inputLists.get(i));
        }

        List<List<Long>> subCross = cross(partialInputLists);

        for (Long firstListVal : firstList) {
            for (List<Long> subCrossList : subCross) {
                List<Long> list = new ArrayList<>();
                list.add(firstListVal);
                for (Long subCrossListVal : subCrossList) {
                    list.add(subCrossListVal);
                }
                cross.add(list);
            }
        }
        return cross;
    }

    private void checkForDuplicates(List<List<Long>> inputLists) {
        Set<Long> valSet = new HashSet<>();
        for (List<Long> list : inputLists) {
            for (Long val : list) {
                if (valSet.contains(val)) {
                    throw new RuntimeException(val + " occured more than once");
                }
                valSet.add(val);
            }
        }
    }
}
