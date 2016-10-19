package com.latticeengines.domain.exposed.dataflow;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.Transient;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.dataflow.annotation.SourceTableName;
import com.latticeengines.domain.exposed.datacloud.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateReportParameters;
import com.latticeengines.domain.exposed.dataflow.flows.CreateScoreTableParameters;
import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.domain.exposed.dataflow.flows.QuotaFlowParameters;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = QuotaFlowParameters.class, name = "quotaFlowParameters"), //
        @JsonSubTypes.Type(value = CreateScoreTableParameters.class, name = "createScoreTableParameters"), //
        @JsonSubTypes.Type(value = CombineInputTableWithScoreParameters.class, name = "combineInputTableWithScoreParameters"), //
        @JsonSubTypes.Type(value = CreateAttributeLevelSummaryParameters.class, name = "createAttributeLevelSummaryParameters"), //
        @JsonSubTypes.Type(value = DedupEventTableParameters.class, name = "dedupEventTableParameters"), //
        @JsonSubTypes.Type(value = AddStandardAttributesParameters.class, name = "addStandardAttributesParameters"), //
        @JsonSubTypes.Type(value = CreateReportParameters.class, name = "createReportParameters"), //
        @JsonSubTypes.Type(value = ParseMatchResultParameters.class, name = "parseMatchResultParameters"), //
        @JsonSubTypes.Type(value = CascadingBulkMatchDataflowParameters.class, name = "cascadingBulkMatchDataflowParameters"), //
})
public class DataFlowParameters {

    private static final Log log = LogFactory.getLog(DataFlowParameters.class);

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
                log.warn(ExceptionUtils.getStackTrace(e));
                // pass
            }
        }
        return sources;
    }
}
