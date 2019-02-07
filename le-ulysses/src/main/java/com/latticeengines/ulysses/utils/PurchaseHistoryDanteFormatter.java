package com.latticeengines.ulysses.utils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;

@Component(PurchaseHistoryDanteFormatter.Qualifier)
public class PurchaseHistoryDanteFormatter {
    public static final String Qualifier = "purchaseHistoryDanteFormatter";

    private class PurchaseHistory {
        private static final String notionName = "DantePurchaseHistory";
        private List<PeriodTransaction> periodTransactions;
        private String accountId;
        private LocalDate periodStartDate;
        private String finalTransactionDate;
        private String firstTransactionDate;

        PurchaseHistory(String accountId, LocalDate startDate, List<PeriodTransaction> periodTransactions,
                String finalTransactionDate, String firstTransactionDate) {
            this.accountId = accountId;
            this.periodStartDate = startDate;
            this.periodTransactions = periodTransactions;
            this.finalTransactionDate = finalTransactionDate;
            this.firstTransactionDate = firstTransactionDate;
        }

        @JsonProperty(value = "AccountID", index = 1)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPeriodOffset() {
            return accountId;
        }

        @JsonProperty(value = "BaseExternalID", index = 2)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getBaseExternalId() {
            return accountId;
        }

        @JsonProperty(value = "NotionName", index = 3)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getNotionName() {
            return notionName;
        }

        @JsonProperty(value = "Period", index = 4)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPeriod() {
            return "M";
        }

        @JsonProperty(value = "PeriodStartDate", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Long getPeriodStartDate() {
            return periodStartDate.atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        }

        @JsonProperty(value = "PurchaseHistoryAttributes", index = 6)
        @JsonView(DanteFormatter.DanteFormat.class)
        @JsonFormat(shape = JsonFormat.Shape.NUMBER)
        public List<DantePurchaseHistoryAttribute> getDantePurchaseHistoryAttributes() {
            return periodTransactions.stream().map(x -> new DantePurchaseHistoryAttribute(accountId, x))
                    .collect(Collectors.toList());
        }

        @JsonProperty(value = "PurchaseHistoryID", index = 7)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPurchaseHistoryID() {
            return accountId;
        }

        @JsonProperty(value = "FinalTransactionDate", index = 8)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Long getFinalTransactionDate() {
            return LocalDate
                    .parse(finalTransactionDate, DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING))
                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        }

        @JsonProperty(value = "FirstTransactionDate", index = 9)
        @JsonView(DanteFormatter.DanteFormat.class)
        public Long getFirstTransactionDate() {
            return LocalDate
                    .parse(firstTransactionDate, DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING))
                    .atStartOfDay(ZoneId.systemDefault()).toEpochSecond();
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this, DanteFormatter.DanteFormat.class);
        }
    }

    private class DantePurchaseHistoryAttribute {
        private PeriodTransaction periodTransaction;
        private String accountId;
        private static final String notionName = "DantePurchaseHistoryAttribute";

        private DantePurchaseHistoryAttribute(String accountId, PeriodTransaction periodTransaction) {
            this.periodTransaction = periodTransaction;
            this.accountId = accountId;
        }

        @JsonProperty(value = "BaseExternalID", index = 1)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getBaseExternalId() {
            return accountId + "_" + periodTransaction.getProductId() + "_" + periodTransaction.getPeriodId();
        }

        @JsonProperty(value = "NotionName", index = 2)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getNotionName() {
            return notionName;
        }

        @JsonProperty(value = "PeriodOffset", index = 3)
        @JsonView(DanteFormatter.DanteFormat.class)
        public int getPeriodOffset() {
            return periodTransaction.getPeriodId();
        }

        @JsonProperty(value = "ProductHierarchyID", index = 4)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getProductHierarchyID() {
            return periodTransaction.getProductId();
        }

        @JsonProperty(value = "PurchaseHistoryAttributeID", index = 5)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPurchaseHistoryAttributeID() {
            return getBaseExternalId();
        }

        @JsonProperty(value = "PurchaseHistoryID", index = 6)
        @JsonView(DanteFormatter.DanteFormat.class)
        public String getPurchaseHistoryID() {
            return accountId;
        }

        @JsonProperty(value = "TotalSpend", index = 7)
        @JsonView(DanteFormatter.DanteFormat.class)
        public double getTotalSpend() {
            return periodTransaction.getTotalAmount();
        }

        @JsonProperty(value = "TotalVolume", index = 8)
        @JsonView(DanteFormatter.DanteFormat.class)
        public double getTotalVolume() {
            return periodTransaction.getTotalQuantity();
        }

        @JsonProperty(value = "TransactionCount", index = 9)
        @JsonView(DanteFormatter.DanteFormat.class)
        public double getTransactionCount() {
            return periodTransaction.getTransactionCount();
        }

        @Override
        public String toString() {
            return JsonUtils.serialize(this, DanteFormatter.DanteFormat.class);
        }
    }

    public String format(String accountId, LocalDate startDate, List<PeriodTransaction> periodTransactions,
            String finalTransactionDate, String firstTransactionDate) {
        return new PurchaseHistory(accountId, startDate, periodTransactions, finalTransactionDate, firstTransactionDate)
                .toString();
    }
}
