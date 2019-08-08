package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;

public class SegmentDependencyUtil {

    public static void findSegmentDependingAttributes(MetadataSegment metadataSegment) {
        Set<AttributeLookup> segmentAttributes = new HashSet<>();
        Set<Restriction> restrictions = getSegmentRestrictions(metadataSegment);
        for (Restriction restriction : restrictions) {
            segmentAttributes.addAll(RestrictionUtils.getRestrictionDependingAttributes(restriction));
        }

        metadataSegment.setSegmentAttributes(segmentAttributes);
    }

    public static Set<Restriction> getSegmentRestrictions(MetadataSegment metadataSegment) {
        Set<Restriction> restrictionSet = new HashSet<>();

        Restriction accountRestriction = metadataSegment.getAccountRestriction();
        if (accountRestriction != null) {
            restrictionSet.add(accountRestriction);
        }

        Restriction contactRestriction = metadataSegment.getContactRestriction();
        if (contactRestriction != null) {
            restrictionSet.add(contactRestriction);
        }

        FrontEndRestriction accountFrontEndRestriction = metadataSegment.getAccountFrontEndRestriction();
        if (accountFrontEndRestriction != null) {
            Restriction accountRestriction1 = accountFrontEndRestriction.getRestriction();
            if (accountRestriction1 != null) {
                restrictionSet.add(accountRestriction1);
            }
        }

        FrontEndRestriction contactFrontEndRestriction = metadataSegment.getContactFrontEndRestriction();
        if (contactFrontEndRestriction != null) {
            Restriction contactRestriction1 = contactFrontEndRestriction.getRestriction();
            if (contactRestriction1 != null) {
                restrictionSet.add(contactRestriction1);
            }
        }

        return restrictionSet;
    }

    public static List<AttributeLookup> findDependingAttributes(List<MetadataSegment> metadataSegments) {
        Set<AttributeLookup> dependingAttributes = new HashSet<>();
        if (metadataSegments != null) {
            for (MetadataSegment metadataSegment : metadataSegments) {
                findSegmentDependingAttributes(metadataSegment);
                Set<AttributeLookup> attributeLookups = metadataSegment.getSegmentAttributes();
                if (attributeLookups != null) {
                    dependingAttributes.addAll(metadataSegment.getSegmentAttributes());
                }
            }
        }

        return new ArrayList<>(dependingAttributes);
    }
}
