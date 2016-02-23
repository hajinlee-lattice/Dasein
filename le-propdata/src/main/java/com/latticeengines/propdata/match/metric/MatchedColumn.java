package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.domain.exposed.propdata.match.InputAccount;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyDimension;
import com.latticeengines.domain.exposed.propdata.match.Matched;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class MatchedColumn extends BaseMeasurement<Matched, MatchedColumn.MatchedColumnDimension>
        implements Measurement<Matched, MatchedColumn.MatchedColumnDimension> {

    private Matched fact;
    private MatchedColumnDimension dimension;

    public MatchedColumn(Boolean matched, String targetColumn, MatchInput input, MatchKeyDimension keyDimension,
            MatchContext.MatchEngine matchEngine) {
        this.fact = new Matched(matched);
        this.dimension = new MatchedColumnDimension(input, keyDimension, matchEngine, targetColumn);
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
    }

    @Override
    public Matched getFact() {
        return fact;
    }

    public void setFact(Matched fact) {
        this.fact = fact;
    }

    @Override
    public MatchedColumnDimension getDimension() {
        return dimension;
    }

    public void setDimension(MatchedColumnDimension dimension) {
        this.dimension = dimension;
    }

    public static class MatchedColumnDimension implements Dimension {
        private String targetColumn;
        private InputAccount inputAccount;

        MatchedColumnDimension(MatchInput input, MatchKeyDimension keyDimension, MatchContext.MatchEngine matchEngine,
                String targetColumn) {
            this.inputAccount = new InputAccount(input, keyDimension);
            if (matchEngine != null) {
                this.inputAccount.setMatchEngine(matchEngine.getName());
            }
            this.targetColumn = targetColumn;
        }

        @MetricTag(tag = "TargetColumn")
        public String getTargetColumn() {
            return targetColumn;
        }

        public void setTargetColumn(String targetColumn) {
            this.targetColumn = targetColumn;
        }

        @MetricTagGroup
        public InputAccount getInputAccount() {
            return inputAccount;
        }

        public void setInputAccount(InputAccount inputAccount) {
            this.inputAccount = inputAccount;
        }
    }

}
