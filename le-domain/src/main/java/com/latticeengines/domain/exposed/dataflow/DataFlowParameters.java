package com.latticeengines.domain.exposed.dataflow;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Transient;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableFilterParameters;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateCdlEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.CreateReportParameters;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.CombineMatchDebugWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.DedupEventTableParameters;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.dataflow.PivotScoreAndEventParameters;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.CreateAttributeLevelSummaryParameters;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.CreateScoreTableParameters;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.QuotaFlowParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = QuotaFlowParameters.class, name = "quotaFlowParameters"), //
        @JsonSubTypes.Type(value = CreateScoreTableParameters.class, name = "createScoreTableParameters"), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreParameters.class, name = "combineInputTableWithScoreParameters"), //
        @JsonSubTypes.Type(value = CombineMatchDebugWithScoreParameters.class, name = "combineMatchDebugWithScoreParameters"), //
        @JsonSubTypes.Type(value = CreateAttributeLevelSummaryParameters.class, name = "createAttributeLevelSummaryParameters"), //
        @JsonSubTypes.Type(value = DedupEventTableParameters.class, name = "dedupEventTableParameters"), //
        @JsonSubTypes.Type(value = AddStandardAttributesParameters.class, name = "addStandardAttributesParameters"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableParameters.class, name = "createCdlEventTableParameters"), //
        @JsonSubTypes.Type(value = CreateCdlEventTableFilterParameters.class, name = "createCdlEventTableFilterParameters"), //
        @JsonSubTypes.Type(value = CreateReportParameters.class, name = "createReportParameters"), //
        @JsonSubTypes.Type(value = ParseMatchResultParameters.class, name = "parseMatchResultParameters"), //
        @JsonSubTypes.Type(value = CascadingBulkMatchDataflowParameters.class, name = "cascadingBulkMatchDataflowParameters"), //
        @JsonSubTypes.Type(value = PivotScoreAndEventParameters.class, name = "pivotScoreAndEventParameters"), //
})
public class DataFlowParameters {

    private static final Logger log = LoggerFactory.getLogger(DataFlowParameters.class);

    @JsonProperty("no_flink")
    public boolean noFlink = false;

    @Transient
    @JsonIgnore
    public final Set<String> getSourceTableNames() {
        List<Field> fields = FieldUtils.getFieldsListWithAnnotation(getClass(), SourceTableName.class);

        Set<String> sources = new HashSet<>();
        for (Field field : fields) {
            try {
                field.setAccessible(true);
                Object value = field.get(this);
                if (value != null) {
                    sources.add(value.toString());
                }
            } catch (IllegalAccessException e) {
                log.warn("Not able to add source for field " + field.getName());
                // pass
            }
        }
        return sources;
    }
}
