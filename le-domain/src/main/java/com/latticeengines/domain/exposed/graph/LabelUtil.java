package com.latticeengines.domain.exposed.graph;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public final class LabelUtil {

    protected LabelUtil() {
        throw new UnsupportedOperationException();
    }

    public static String concatenateLabels(List<String> labelList) {
        String concatenatedLabel = null;

        if (CollectionUtils.isNotEmpty(labelList)) {
            StringBuilder sb = new StringBuilder();
            labelList.stream().forEach(l -> {
                if (StringUtils.isBlank(l)) {
                    throwException("label cannot be null or empty");
                }
                if (l.contains(GraphConstants.LABEL_SEP)) {
                    throwException(
                            String.format("label should not contain %s", GraphConstants.LABEL_SEP));
                }
                sb.append(l);
                sb.append(GraphConstants.LABEL_SEP);
            });

            concatenatedLabel = sb.toString();
            concatenatedLabel = concatenatedLabel.substring(0,
                    concatenatedLabel.lastIndexOf(GraphConstants.LABEL_SEP));
        } else {
            throwException("labelList should not be empty");
        }
        return concatenatedLabel;
    }

    public static List<String> extractLabels(String label) {
        List<String> labels = new ArrayList<>();
        if (StringUtils.isNotBlank(label) //
                && label.contains(GraphConstants.LABEL_SEP)) {
            String remainingPart = label;
            while (remainingPart.contains(GraphConstants.LABEL_SEP)) {
                labels.add(remainingPart.substring(0,
                        remainingPart.indexOf(GraphConstants.LABEL_SEP)));

                remainingPart = remainingPart
                        .substring(remainingPart.indexOf(GraphConstants.LABEL_SEP) //
                                + GraphConstants.LABEL_SEP.length());
            }
            labels.add(remainingPart);
        } else {
            labels.add(label);
        }
        return labels;
    }

    private static void throwException(String msg) {
        throw new RuntimeException(msg);
    }
}
