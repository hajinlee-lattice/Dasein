package com.latticeengines.datacloud.dataflow.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.dataflow.AttrDimension;

public class DimensionUtilsUnitTestNG {

    @Test(groups = "unit")
    public void test() {
        AttrDimension A1 = new AttrDimension("A1");
        AttrDimension A2 = new AttrDimension("A2");
        AttrDimension A3 = new AttrDimension("A3");
        A1.setChildren(Arrays.asList(A2, A3));

        AttrDimension B1 = new AttrDimension("B1");
        AttrDimension B2 = new AttrDimension("B2");
        AttrDimension B3 = new AttrDimension("B3");
        B1.setChildren(Collections.singleton(B2));
        B2.setChildren(Collections.singleton(B3));

        AttrDimension C = new AttrDimension("C");
        List<AttrDimension> dimTree = Arrays.asList(A1, A2, A3, B1, B2, B3, C);
        Collections.shuffle(dimTree);
        List<List<AttrDimension>> paths = DimensionUtils.getRollupPaths(dimTree);
        Assert.assertEquals(paths.size(), 39);
        List<List<AttrDimension>> seenPaths = new ArrayList<>();
        paths.forEach(path -> {
            if (path.size() > 1) {
                AttrDimension currDim = path.get(path.size() - 1);
                Assert.assertTrue(path.containsAll(currDim.getChildren()));
                AtomicBoolean seenParentPath = new AtomicBoolean(false);
                seenPaths.forEach(seenPath -> {
                    if (seenPath.size() != path.size() - 1) {
                        return;
                    }
                    if (DimensionUtils.pathToString(seenPath)
                            .equals(DimensionUtils.pathToString(path.subList(0, path.size() - 1)))) {
                        seenParentPath.set(true);
                    }
                });
                Assert.assertTrue(seenParentPath.get(),
                        "Have not seen parent path of " + DimensionUtils.pathToString(path));
            } else {
                Assert.assertTrue(path.get(0).getChildren().isEmpty(),
                        "Non-leaf dim " + path.get(0).getName() + " should not be the first in a path.");
            }
            seenPaths.add(path);
        });

        // random test
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 100; i++) {
            Collections.shuffle(dimTree);
            List<AttrDimension> randomCombo = new ArrayList<>(
                    dimTree.subList(0, 1 + random.nextInt(dimTree.size() - 1)));
            if (isValidCombo(randomCombo)) {
                boolean inPaths = false;
                for (List<AttrDimension> path : paths) {
                    if (path.size() == randomCombo.size() && path.containsAll(randomCombo)) {
                        inPaths = true;
                        break;
                    }
                }
                Assert.assertTrue(inPaths,
                        "Did not find valid combination [" + StringUtils.join(
                                randomCombo.stream().map(AttrDimension::getName).collect(Collectors.toList()), ",")
                                + "]");
            }
        }
    }

    private boolean isValidCombo(List<AttrDimension> randomCombo) {
        for (AttrDimension dim: randomCombo) {
            if (!dim.getChildren().isEmpty() && !randomCombo.containsAll(dim.getChildren())) {
                return false;
            }
        }
        return true;
    }

}
