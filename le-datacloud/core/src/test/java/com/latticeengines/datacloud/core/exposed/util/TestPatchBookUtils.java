package com.latticeengines.datacloud.core.exposed.util;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;

/**
 * Utility class for testing {@link PatchBook} related stuff
 */
public class TestPatchBookUtils {

    /*
     * Generated MatchKeyTuple for testing, naming convention is <NS>_<MATCH_KEY>_<GROUP>_<OPTIONAL_ID>?
     * NS: namespace used to group related tuples (e.g., GOOGLE for all google related match key tuples)
     * MATCH_KEY: identify which match keys are set
     *   - D: Domain
     *   - DU: DUNS
     *   - N: Name
     *   - C: Country
     *   - S: State
     *   - CI: City
     *   - Z: Zipcode
     * GROUP: tuples with the same NS, MATCH_KEY, GROUP will have the same match key values after standardization.
     *     (e.g., Name=google & Name=GooGLe will be in the same group)
     * OPTIONAL_ID: optional, used to prevent name conflict when there are more than 1 tuples in the group
     *
     * NOTE: Country code must be set whenever Country is set (for KeyPartition evaluation)
     */

    /* GOOGLE */

    public static final MatchKeyTuple GOOGLE_N_1_1 = new MatchKeyTuple.Builder()
            .withName("Google").build();
    public static final MatchKeyTuple GOOGLE_N_1_2 = new MatchKeyTuple.Builder()
            .withName("google").build();
    public static final MatchKeyTuple GOOGLE_N_1_3 = new MatchKeyTuple.Builder()
            .withName("GOOGLE  ").build();
    public static final MatchKeyTuple GOOGLE_NC_1_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").build();
    public static final MatchKeyTuple GOOGLE_NC_1_2 = new MatchKeyTuple.Builder()
            .withName("google").withCountry("United States").withCountryCode("USA").build();
    public static final MatchKeyTuple GOOGLE_NCS_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withState("CA").build();
    public static final MatchKeyTuple GOOGLE_NCS_2 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withState("NY").build();
    public static final MatchKeyTuple GOOGLE_NCC_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withCity("Sunnyvale").build();
    public static final MatchKeyTuple GOOGLE_NCSCI_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withState("CA").withCity("Sunnyvale").build();
    public static final MatchKeyTuple GOOGLE_NCSCI_2 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA")
            .withState("CA").withCity("Mountain View").build();
    public static final MatchKeyTuple GOOGLE_NSCI_1 = new MatchKeyTuple.Builder()
            .withName("Google").withState("CA").withCity("Sunnyvale").build();
    public static final MatchKeyTuple GOOGLE_NSCI_2 = new MatchKeyTuple.Builder()
            .withName("Google").withState("CA").withCity("Mountain View").build();
    public static final MatchKeyTuple GOOGLE_NSCI_3 = new MatchKeyTuple.Builder()
            .withName("Google").withState("CA").withCity("San Jose").build();
    public static final MatchKeyTuple GOOGLE_NS_1 = new MatchKeyTuple.Builder()
            .withName("Google").withState("CA").build();
    public static final MatchKeyTuple GOOGLE_NS_2 = new MatchKeyTuple.Builder()
            .withName("Google").withState("NY").build();
    public static final MatchKeyTuple GOOGLE_NCZ_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withZipcode("94039").build();
    public static final MatchKeyTuple GOOGLE_NCSZ_1 = new MatchKeyTuple.Builder()
            .withName("Google").withCountry("USA").withCountryCode("USA").withState("CA").withZipcode("94039").build();
    public static final MatchKeyTuple GOOGLE_D_1_1 = new MatchKeyTuple.Builder()
            .withDomain("google.com").build();
    public static final MatchKeyTuple GOOGLE_D_1_2 = new MatchKeyTuple.Builder()
            .withDomain("www.google.com").build();
    public static final MatchKeyTuple GOOGLE_DC_1_1 = new MatchKeyTuple.Builder()
            .withDomain("google.com").withCountry("USA").withCountryCode("USA").build();
    public static final MatchKeyTuple GOOGLE_DC_1_2 = new MatchKeyTuple.Builder()
            .withDomain("www.google.com").withCountry("USA").withCountryCode("USA").build();
    public static final MatchKeyTuple GOOGLE_DN_1 = new MatchKeyTuple.Builder()
            .withDomain("google.com").withName("Google").build();
    public static final MatchKeyTuple GOOGLE_DN_2 = new MatchKeyTuple.Builder()
            .withDomain("www.google.com").withName("Google Inc").build();
    public static final MatchKeyTuple GOOGLE_DU_1 = new MatchKeyTuple.Builder().withDuns("060902413").build();

    /* FACEBOOK */

    public static final MatchKeyTuple FACEBOOK_N_1_1 = new MatchKeyTuple.Builder()
            .withName("Facebook").build();
    public static final MatchKeyTuple FACEBOOK_N_1_2 = new MatchKeyTuple.Builder()
            .withName("facebook").build();
    public static final MatchKeyTuple FACEBOOK_N_1_3 = new MatchKeyTuple.Builder()
            .withName("  FacEBOok  ").build();
    public static final MatchKeyTuple FACEBOOK_NC_1 = new MatchKeyTuple.Builder()
            .withName("Facebook").withCountry("USA").withCountryCode("USA").build();
    public static final MatchKeyTuple FACEBOOK_NCS_1 = new MatchKeyTuple.Builder()
            .withName("Facebook").withCountry("USA").withCountryCode("USA").withState("CA").build();
    public static final MatchKeyTuple FACEBOOK_NCSCI_1 = new MatchKeyTuple.Builder()
            .withName("Facebook").withCountry("USA").withCountryCode("USA")
            .withState("CA").withZipcode("94404").build();
    public static final MatchKeyTuple FACEBOOK_NSCI_1 = new MatchKeyTuple.Builder()
            .withName("Facebook").withState("CA").withZipcode("94404").build();
    public static final MatchKeyTuple FACEBOOK_D_1 = new MatchKeyTuple.Builder()
            .withDomain("facebook.com").build();
    public static final MatchKeyTuple FACEBOOK_DC_1 = new MatchKeyTuple.Builder()
            .withDomain("facebook.com").withCountry("USA").withCountryCode("USA").build();
    public static final MatchKeyTuple FACEBOOK_DC_2 = new MatchKeyTuple.Builder()
            .withDomain("facebook.com").withCountry("United Kingdom").withCountryCode("UK").build();
    public static final MatchKeyTuple FACEBOOK_DCS_1 = new MatchKeyTuple.Builder()
            .withDomain("facebook.com").withCountry("USA").withCountryCode("USA").withState("CA").build();
    public static final MatchKeyTuple FACEBOOK_DCZ_1 = new MatchKeyTuple.Builder()
            .withDomain("facebook.com").withCountry("USA").withCountryCode("USA").withZipcode("94404").build();

    /**
     * Create {@link PatchBook} with specified inputs, {@link PatchBook.Type} will be {@literal null}
     * @param patchBookId primary ID
     * @param tuple used to set input match keys, should not be {@literal null}
     * @param patchItems patchItems instance
     * @return created {@link PatchBook} instance
     */
    public static PatchBook newPatchBook(long patchBookId, @NotNull MatchKeyTuple tuple,
            Map<String, Object> patchItems) {
        return newPatchBook(patchBookId, null, false, tuple, patchItems);
    }

    /**
     * Create {@link PatchBook} with specified inputs
     * @param patchBookId primary ID
     * @param type patch book type
     * @param tuple used to set input match keys, should not be {@literal null}
     * @param patchItems patchItems instance
     * @return created {@link PatchBook} instance
     */
    public static PatchBook newPatchBook(
            long patchBookId, PatchBook.Type type, @NotNull Boolean isCleanup, @NotNull MatchKeyTuple tuple, Map<String, Object> patchItems) {
        PatchBook book = new PatchBook();
        book.setPid(patchBookId);
        book.setType(type);
        book.setCleanup(isCleanup);
        book.setDomain(tuple.getDomain());
        book.setDuns(tuple.getDuns());
        book.setName(tuple.getName());
        book.setCountry(tuple.getCountry());
        book.setState(tuple.getState());
        book.setCity(tuple.getCity());
        book.setZipcode(tuple.getZipcode());
        // copy patchItems
        if (patchItems != null) {
            book.setPatchItems(new HashMap<>(patchItems));
        }
        return book;
    }

    /**
     * Create a patchItems with DUNS field set to the input DUNS value
     * @param duns input DUNS value
     * @return created patchItems
     */
    public static Map<String, Object> newDunsPatchItems(String duns) {
        return newDomainDunsPatchItems(null, duns);
    }

    /**
     * Create a patchItems with Domain field set to the input domain value
     * @param domain input domain value
     * @return created patchItems
     */
    public static Map<String, Object> newDomainPatchItems(String domain) {
        return newDomainDunsPatchItems(domain, null);
    }

    /**
     * Create a patchItems with Domain and DUNS field set to the input values
     * @param domain input domain value
     * @param duns input DUNS value
     * @return created patchItems
     */
    public static Map<String, Object> newDomainDunsPatchItems(String domain, String duns) {
        Map<String, Object> items = new HashMap<>();
        if (domain != null) {
            items.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Domain), domain);
        }
        if (duns != null) {
            items.put(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.DUNS), duns);
        }
        return items;
    }
}
