package com.latticeengines.cdl.workflow.steps.play;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
class RecommendationCreator {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchProcessor.class);

    void generateRecommendations(PlayLaunchContext playLaunchContext, List<Map<String, Object>> accountList,
                                 Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
                                 DataFileWriter<GenericRecord> dataFileWriter) {

        List<Recommendation> recommendations = accountList//
                .stream().parallel() //
                .map( //
                        account -> {
                            try {
                                return processSingleAccount(playLaunchContext, //
                                        mapForAccountAndContactList, account);
                            } catch (Throwable th) {
                                log.error(th.getMessage(), th);
                                playLaunchContext.getCounter().getAccountErrored().addAndGet(1);
                                return null;
                            }
                        }) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList());

        if (CollectionUtils.isNotEmpty(recommendations)) {
            List<GenericRecord> records = //
                    recommendations.stream() //
                            .map(rec -> createRecommendationRecord(dataFileWriter, playLaunchContext.getSchema(), rec)) //
                            .collect(Collectors.toList());

            records.forEach(datum -> {
                        try {
                            dataFileWriter.append(datum);
                        } catch (IOException e) {
                            log.error(String.format("Error while writing recommendation record (%s) to avro file",
                                    JsonUtils.serialize(datum)), e);
                        }
                    });
        }
    }

    private GenericRecord createRecommendationRecord(DataFileWriter<GenericRecord> dataFileWriter, Schema schema,
            Recommendation recommendation) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        Map<String, Object> recMap = Recommendation.convertToMap(recommendation);

        for (Field field : schema.getFields()) {
            String fieldName = field.name();
            builder.set(fieldName, recMap.get(fieldName));
        }
        return builder.build();
    }

    private Recommendation processSingleAccount(PlayLaunchContext playLaunchContext,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, Map<String, Object> account) {
        Recommendation recommendation;

        // prepare recommendation
        recommendation = //
                prepareRecommendation(playLaunchContext, account, mapForAccountAndContactList);

        // update corresponding counters
        playLaunchContext.getCounter().getContactLaunched().addAndGet(
                recommendation.getExpandedContacts() != null ? recommendation.getExpandedContacts().size() : 0);
        playLaunchContext.getCounter().getAccountLaunched().addAndGet(1);

        return recommendation;
    }

    private Recommendation prepareRecommendation(PlayLaunchContext playLaunchContext, Map<String, Object> account,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList) {
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        long launchTimestampMillis = playLaunchContext.getLaunchTimestampMillis();
        String playName = playLaunchContext.getPlayName();
        String playLaunchId = playLaunchContext.getPlayLaunchId();
        Tenant tenant = playLaunchContext.getTenant();

        Object accountId = checkAndGet(account, InterfaceName.AccountId.name());
        if (accountId == null) {
            throw new RuntimeException("Account Id can not be null");
        }

        Recommendation recommendation = new Recommendation();
        recommendation.setRecommendationId(UUID.randomUUID().toString());
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(accountId.toString());
        recommendation.setLeAccountExternalID(accountId.toString());

        if (StringUtils.isNotBlank(playLaunch.getDestinationAccountId())) {
            String destinationAccountId = playLaunch.getDestinationAccountId().trim();
            recommendation.setSfdcAccountID(checkAndGet(account, destinationAccountId));
        } else {
            recommendation.setSfdcAccountID(null);
        }

        // give preference to lattice data cloud field LDC_Name. If not found
        // then try to get company name from customer data itself.
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        if (StringUtils.isBlank(recommendation.getCompanyName())) {
            recommendation.setCompanyName(checkAndGet(account, InterfaceName.LDC_Name.name()));
        }

        recommendation.setTenantId(tenant.getPid());

        if (playLaunchContext.getRatingId() != null) {
            String score = checkAndGet(account,
                    playLaunchContext.getRatingId() + PlaymakerConstants.RatingScoreColumnSuffix);
            String bucketName = checkAndGet(account, playLaunchContext.getRatingId(),
                    RatingBucketName.getUnscoredBucketName());
            RatingBucketName bucket = EnumUtils.isValidEnum(RatingBucketName.class, bucketName)
                    ? RatingBucketName.valueOf(bucketName)
                    : null;
            recommendation.setLikelihood(
                    StringUtils.isNotEmpty(score) ? Double.parseDouble(score) : getDefaultLikelihood(bucket));

            String expectedValue = checkAndGet(account,
                    playLaunchContext.getRatingId() + PlaymakerConstants.RatingEVColumnSuffix);
            recommendation
                    .setMonetaryValue(StringUtils.isNotEmpty(expectedValue) ? Double.parseDouble(expectedValue) : null);

            setSyncDestination(playLaunch, recommendation);

            recommendation.setPriorityID(bucket);
            recommendation.setPriorityDisplayName(bucketName);

            recommendation.setRatingModelId(playLaunchContext.getPublishedIteration().getId());
            recommendation.setModelSummaryId(
                    playLaunchContext.getPlay().getRatingEngine().getType() != RatingEngineType.RULE_BASED
                            ? ((AIModel) playLaunchContext.getPublishedIteration()).getModelSummaryId()
                            : "");

        }
        if (mapForAccountAndContactList.containsKey(accountId)) {
            List<Map<String, String>> contactsForRecommendation = PlaymakerUtils
                    .generateContactForRecommendation(mapForAccountAndContactList.get(accountId));
            recommendation.setExpandedContacts(contactsForRecommendation);
        }

        return recommendation;
    }

    private void setSyncDestination(PlayLaunch playLaunch, Recommendation recommendation) {
        String synchronizationDestination;
        String destinationSysType;
        if (playLaunch.getDestinationSysType() == null
                || playLaunch.getDestinationSysType() == CDLExternalSystemType.CRM) {
            synchronizationDestination = SynchronizationDestinationEnum.SFDC.name();
            destinationSysType = CDLExternalSystemType.CRM.name();
        } else if (playLaunch.getDestinationSysType() == CDLExternalSystemType.MAP) {
            synchronizationDestination = SynchronizationDestinationEnum.MAP.name();
            destinationSysType = CDLExternalSystemType.MAP.name();
        } else if (playLaunch.getDestinationSysType() == CDLExternalSystemType.FILE_SYSTEM) {
            synchronizationDestination = SynchronizationDestinationEnum.FILE_SYSTEM.name();
            destinationSysType = CDLExternalSystemType.FILE_SYSTEM.name();
        } else {
            throw new RuntimeException(String.format("Destination type %s is not supported yet",
                    playLaunch.getDestinationSysType().name()));
        }

        recommendation.setSynchronizationDestination(synchronizationDestination);
        if (StringUtils.isNotBlank(playLaunch.getDestinationOrgId())) {
            recommendation.setDestinationOrgId(playLaunch.getDestinationOrgId());
            recommendation.setDestinationSysType(destinationSysType);
        }
    }

    private String checkAndGet(Map<String, Object> account, String columnName) {
        return checkAndGet(account, columnName, null);
    }

    private String checkAndGet(Map<String, Object> account, String columnName, String defaultValue) {
        return account.get(columnName) != null ? account.get(columnName).toString() : defaultValue;
    }

    private static double getDefaultLikelihood(RatingBucketName bucket) {
        if (bucket == null)
            return 0;

        switch (bucket) {
            case A:
                return 95.0D;
            case B:
                return 70.0D;
            case C:
                return 40.0D;
            case D:
                return 20.0D;
            case E:
                return 10.0D;
            case F:
                return 5.0D;
            default:
                throw new UnsupportedOperationException("Unknown bucket " + bucket);
        }
    }

}
