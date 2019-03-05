package com.latticeengines.datacloud.workflow.match.steps;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.query.BusinessEntity.CuratedAccount;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Product;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.batch.item.ExecutionContext;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps.CommitEntityMatchConfiguration;

public class CommitEntityMatchUnitTestNG {

    /*
     * Test CommitEntityMatch#getEntitySet method, which return a set of entity that
     * requires publishing their staging store to serving store. For simplicity,
     * this unit test does not test user-specified entity list in config.
     */
    @Test(groups = "unit", dataProvider = "getEntitySet")
    private void testGetEntitySet(CommitEntityMatchConfiguration config, BusinessEntity[] entitiesWithImport,
            BusinessEntity[] publishedEntities, BusinessEntity[] expectedEntitiesToCommit) {
        CommitEntityMatch commitEntityMatch = mockCommitEntityMatch(config, entitiesWithImport);
        Set<String> entitiesToCommit = commitEntityMatch
                .getEntitySet(Arrays.stream(publishedEntities).map(Enum::name).collect(Collectors.toSet()));
        Assert.assertNotNull(entitiesToCommit);
        Assert.assertEquals(entitiesToCommit,
                Arrays.stream(expectedEntitiesToCommit).map(Enum::name).collect(Collectors.toSet()));
    }

    @DataProvider(name = "getEntitySet")
    private Object[][] provideGetEntitySetTestData() {
        return new Object[][] {
                /*
                 * check all entities for import & skip published entities
                 */
                { //
                        /*
                         * Account is already published, so only publish Contact & Product
                         */
                        newConfig(true, true), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[] { Contact, Product }, //
                }, //
                { //
                        /*
                         * some entities that does not have import but is already published
                         * (CuratedAccount). this can happen due to previous CommitEntityMatch with
                         * force rebuild entity
                         */
                        newConfig(true, true), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account, CuratedAccount, Contact }, //
                        new BusinessEntity[] { Product }, //
                }, //
                { //
                        /*
                         * some entities that does not have import but is already published (rebuilt
                         * entity), also all entities that have import already published
                         */
                        newConfig(true, true), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account, Contact, Product, CuratedAccount }, //
                        new BusinessEntity[0], //
                }, //
                { //
                        newConfig(true, true), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact, Product }, //
                }, //
                { //
                        newConfig(true, true), //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[0], //
                }, //
                /*
                 * check all entities for import & NOT skip published entities (all entities
                 * with import will be published)
                 */
                { //
                        newConfig(true, false), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[] { Account, Contact, Product }, //
                }, //
                { //
                        newConfig(true, false), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account, Contact, Product, CuratedAccount }, //
                        new BusinessEntity[] { Account, Contact, Product }, //
                }, //
                { //
                        newConfig(true, false), //
                        new BusinessEntity[] { Product }, //
                        new BusinessEntity[] { Account, Contact, Product, CuratedAccount }, //
                        new BusinessEntity[] { Product }, //
                }, //
                { //
                        newConfig(true, false), //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact, Product, CuratedAccount }, //
                        new BusinessEntity[0], //
                }, //
                { //
                        newConfig(true, false), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact, Product }, //
                }, //
                /*
                 * check specified list of entities for import & skip published entities
                 */
                { //
                        /*
                         * product is not in the specified list, so not publishing even if it has import
                         */
                        newConfig(false, true, Account, Contact), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact }, //
                }, //
                { //
                        /*
                         * product is not in the specified list and account is already published, so
                         * only publishing contact
                         */
                        newConfig(false, true, Account, Contact), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[] { Contact }, //
                }, //
                { //
                        /*
                         * empty specified list, not publishing any entity even if some has import
                         */
                        newConfig(false, true), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[0], //
                }, //
                /*
                 * check specified list of entities for import & NOT skip published entities
                 */
                { //
                        newConfig(false, false, Account, Contact), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[0], //
                        new BusinessEntity[] { Account, Contact }, //
                }, //
                { //
                        newConfig(false, false, Account, Contact), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[] { Account, Contact }, //
                }, //
                { //
                        newConfig(false, false), //
                        new BusinessEntity[] { Account, Contact, Product }, //
                        new BusinessEntity[] { Account }, //
                        new BusinessEntity[0], //
                }, //
        };
    }

    /*
     * helper for constructing config & test class
     */

    private CommitEntityMatch mockCommitEntityMatch(CommitEntityMatchConfiguration config,
            BusinessEntity[] entitiesWithImport) {
        CommitEntityMatch commitEntityMatch = new CommitEntityMatch();
        commitEntityMatch.setExecutionContext(new ExecutionContext());
        if (entitiesWithImport != null) {
            Map<String, ArrayList<DataFeedImport>> importMap = Arrays.stream(entitiesWithImport) //
                    .map(Enum::name) //
                    .map(entity -> Pair.of(entity, new ArrayList<DataFeedImport>())) //
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            commitEntityMatch.putObjectInContext(CommitEntityMatch.CONSOLIDATE_INPUT_IMPORTS, importMap);
        }
        commitEntityMatch.setConfiguration(config);
        return commitEntityMatch;
    }

    private CommitEntityMatchConfiguration newConfig(boolean checkAllEntityForImport, boolean skipPublished,
            BusinessEntity... entitiesToCheckImport) {
        if (entitiesToCheckImport.length > 0) {
            // flag has to be false for entities to check import to take effect
            Preconditions.checkArgument(!checkAllEntityForImport);
        }
        CommitEntityMatchConfiguration config = new CommitEntityMatchConfiguration();
        config.setCheckAllEntityImport(checkAllEntityForImport);
        config.setSkipPublishedEntities(skipPublished);
        config.setEntityImportSetToCheck(
                Arrays.stream(entitiesToCheckImport).map(Enum::name).collect(Collectors.toSet()));
        return config;
    }
}
