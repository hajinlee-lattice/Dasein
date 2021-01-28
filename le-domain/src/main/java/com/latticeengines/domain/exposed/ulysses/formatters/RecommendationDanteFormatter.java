package com.latticeengines.domain.exposed.ulysses.formatters;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

@Component(RecommendationDanteFormatter.Qualifier)
public class RecommendationDanteFormatter implements DanteFormatter<Recommendation> {

    public static final String Qualifier = "recommendationDanteFormatter";
    private static final String notionName = "DanteLead";

    @Override
    public String format(Recommendation entity) {
        if (entity == null)
            return null;
        return new DanteLead(entity).toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<String> format(List<Recommendation> entities) {
        return entities != null //
                ? entities.stream().filter(entity -> entity != null).map(this::format).collect(Collectors.toList()) //
                : Collections.EMPTY_LIST;
    }

    private class DanteLead {

        private Recommendation recommendation;

        private DanteLead(Recommendation recommendation) {
            this.recommendation = recommendation;
        }

        @JsonProperty(value = "BaseExternalID", index = 1)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getBaseExternalID() {
            return recommendation.getId();
        }

        @JsonProperty(value = "NotionName", index = 2)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getNotionName() {
            return notionName;
        }

        @JsonProperty(value = "AnalyticAttributes", index = 3)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Object[] getAnalyticAttributes() {
            return new Object[0];
        }

        @JsonProperty(value = "DisplayName", index = 4)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getDisplayName() {
            return recommendation.getCompanyName();
        }

        @JsonProperty(value = "ExpectedValue", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Double getExpectedValue() {
            return recommendation.getMonetaryValue();
        }

        @JsonProperty(value = "ExternalProbability", index = 6)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Double getExternalProbability() {
            return recommendation.getLikelihood();
        }

        @JsonProperty(value = "LastLaunched", index = 7)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Date getLastLaunched() {
            return recommendation.getLaunchDate();
        }

        @JsonProperty(value = "LastModified", index = 8)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Date getLastModified() {
            return recommendation.getLastUpdatedTimestamp();
        }

        @JsonProperty(value = "LeadID", index = 9)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getLeadID() {
            return recommendation.getId();
        }

        @JsonProperty(value = "Lift", index = 10)
        @JsonView(DanteFormatter.DanteFormat.class)
        public double getLift() {
            return 0;
        }

        @JsonProperty(value = "LikelihoodBucketDisplayName", index = 11)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getLikelihoodBucketDisplayName() {
            return recommendation.getPriorityDisplayName();
        }

        @JsonProperty(value = "LikelihoodBucketOffset", index = 12)
        @JsonView(DanteFormatter.DanteFormat.class)
        public int getLikelihoodBucketOffset() {
            return 0;
        }

        @JsonProperty(value = "ModelID", index = 13)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getModelID() {
            return "";
        }

        @JsonProperty(value = "Percentile", index = 14)
        @JsonView(DanteFormatter.DanteFormat.class)
        public int getPercentile() {
            return 0;
        }

        @JsonProperty(value = "PlayDescription", index = 15)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayDescription() {
            return recommendation.getDescription();
        }

        @JsonProperty(value = "PlayDisplayName", index = 16)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayDisplayName() {
            return recommendation.getPlayId();
        }

        @JsonProperty(value = "PlayID", index = 17)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayID() {
            return recommendation.getPlayId();
        }

        @JsonProperty(value = "PlaySolutionType", index = 18)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlaySolutionType() {
            return "";
        }

        @JsonProperty(value = "PlayTargetProductName", index = 19)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayTargetProductName() {
            return "";
        }

        @JsonProperty(value = "PlayType", index = 20)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPlayType() {
            return "";
        }

        @JsonProperty(value = "Probability", index = 21)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Double getProbability() {
            return recommendation.getLikelihood();
        }

        @JsonProperty(value = "Rank", index = 22)
        @JsonView(DanteFormatter.DanteFormat.class)
        public int getRank() {
            return 0;
        }

        @JsonProperty(value = "RecommendationID", index = 23)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getRecommendationID() {
            return recommendation.getId();
        }

        @JsonProperty(value = "SalesforceAccountID", index = 24)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getSalesforceAccountID() {
            if (StringUtils.isNotBlank(recommendation.getSfdcAccountID()))
                return recommendation.getSfdcAccountID();
            else if (StringUtils.isNotBlank(recommendation.getLeAccountExternalID()))
                return recommendation.getLeAccountExternalID();
            else
                return recommendation.getAccountId();
        }

        @JsonProperty(value = "SfdcID", index = 25)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getSfdcID() {
            return "";
        }

        @JsonProperty(value = "Theme", index = 26)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getTheme() {
            return "";
        }

        @JsonProperty(value = "UserRoleDisplayName", index = 27)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getUserRoleDisplayName() {
            return "";
        }

        @JsonProperty(value = "HeaderName", index = 28)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String HeaderName() {
            return "";
        }

        @JsonProperty(value = "Timestamp", index = 29)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Date Timestamp() {
            return new Date();
        }

        @JsonProperty(value = "TalkingPoints", index = 30)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Object[] getTalkingPoints() {
            return new Object[0];
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this, DanteFormatter.DanteFormat.class);
        }
    }
}
