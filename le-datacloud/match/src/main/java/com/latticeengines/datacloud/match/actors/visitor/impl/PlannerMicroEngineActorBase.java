package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ExecutorMicroEngineTemplate;
import com.latticeengines.common.exposed.util.ValidationUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class PlannerMicroEngineActorBase extends ExecutorMicroEngineTemplate {

    private static final Logger log = LoggerFactory.getLogger(PlannerMicroEngineActorBase.class);

    /**
     * Standardize all match key fields and create {@link MatchKeyTuple} for current
     * entity
     *
     * @param traveler
     *            traveler object for current record
     */
    protected abstract void standardizeMatchFields(@NotNull MatchTraveler traveler);

    @Override
    protected void execute(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        standardizeMatchFields(matchTraveler);

        boolean isValid = validateMatchFields(matchTraveler);
        if (!isValid) {
            // only set if some match field is invalid, cuz planner in other graph could
            // have set this field already
            log.debug("Invalid match field values found in entity {}", matchTraveler.getEntity());
            matchTraveler.setHasInvalidValue(true);
        }

        if (Boolean.TRUE.equals(matchTraveler.getHasInvalidValue())) {
            // when any match field is invalid, we do nothing and return anonymous entity
            // (by cleaning up all match key tuple fields)
            // if we use partial (valid) feilds to match, we could messed up match result
            // TODO maybe only cleanup in allocateId mode
            log.debug("Cleaning match key tuple to return anonymous {}", matchTraveler.getEntity());
            MatchKeyTuple tuple = new MatchKeyTuple();
            matchTraveler.setMatchKeyTuple(tuple);
            // replace all existing tuple with the empty one
            matchTraveler.getEntityMatchKeyTuples() //
                    .forEach((key, oldTuple) -> matchTraveler.addEntityMatchKeyTuple(key, tuple));
            if (BusinessEntity.Account.name().equals(matchTraveler.getEntity())) {
                // prevent going into LDC match // FIXME find a better way
                matchTraveler.addEntityMatchKeyTuple(BusinessEntity.LatticeAccount.name(), tuple);
            }

        }
    }

    /*
     * Validate all match fields in current match key tuple and add error message if
     * some match field value is invalid. Return true if all fields are valid.
     */
    protected boolean validateMatchFields(@NotNull MatchTraveler traveler) {
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();
        EntityMatchKeyRecord record = traveler.getEntityMatchKeyRecord();
        if (tuple == null) {
            return true;
        }

        boolean res = true;
        res &= validateMatchField(MatchKey.Domain, tuple.getDomain(), record);
        res &= validateMatchField(MatchKey.Name, tuple.getName(), record);
        res &= validateMatchField(MatchKey.Country, tuple.getCountry(), record);
        res &= validateMatchField(MatchKey.State, tuple.getState(), record);
        res &= validateMatchField(MatchKey.City, tuple.getCity(), record);
        res &= validateMatchField(MatchKey.Zipcode, tuple.getZipcode(), record);
        res &= validateMatchField(MatchKey.PhoneNumber, tuple.getPhoneNumber(), record);
        res &= validateMatchField(MatchKey.DUNS, tuple.getDuns(), record);
        res &= validateMatchField(MatchKey.Email, tuple.getEmail(), record);
        if (record != null && MapUtils.isNotEmpty(record.getParsedPreferredEntityIds())) {
            res &= validateMatchField(MatchKey.PreferredEntityId,
                    record.getParsedPreferredEntityIds().get(traveler.getEntity()), record);
        }
        if (CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
            Optional<Boolean> valid = tuple.getSystemIds() //
                    .stream() //
                    .filter(Objects::nonNull) //
                    /*-
                     * validate both system name and ID value
                     */
                    .map(pair -> validateMatchField(MatchKey.SystemId, pair.getKey(), record) //
                            && validateMatchField(MatchKey.SystemId, pair.getValue(), record)) //
                    .reduce(Boolean::logicalAnd);
            if (valid.isPresent()) {
                res &= valid.get();
            }
        }
        return res;
    }

    private boolean validateMatchField(@NotNull MatchKey matchKey, String value, EntityMatchKeyRecord record) {
        boolean valid = ValidationUtils.isValidMatchFieldValue(value);
        if (!valid && record != null) {
            record.addErrorMessages(String.format("value %s for match key %s is invalid", value, matchKey.name()));
        }
        return valid;
    }

}
