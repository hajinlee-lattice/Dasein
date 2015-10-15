package com.latticeengines.propdata.collection.dataflow.merge;

import java.util.Collections;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;

import cascading.tuple.Fields;

@Component("featureMergeRawSnapshotDataFlowBuilder")
@Scope("prototype")
public class FeatureMergeRawSnapshotDataFlowBuilder extends MergeRawSnapshotDataFlowBuilder {

    private static final FeatureArchiveProgress progress = new FeatureArchiveProgress();

    @Override
    protected Fields uniqueFields() {
        Fields fields = new Fields("URL", "Feature");
        fields.setComparator("URL", String.CASE_INSENSITIVE_ORDER);
        fields.setComparator("Feature", String.CASE_INSENSITIVE_ORDER);
        return fields;
    }

    @Override
    protected Fields sortFields() {
        Fields timestampField = new Fields("LE_Last_Upload_Date");
        timestampField.setComparator(timestampField, Collections.reverseOrder());
        return timestampField;
    }
}
