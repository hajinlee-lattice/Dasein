package com.latticeengines.domain.exposed.mds;

import java.util.Comparator;

import org.apache.commons.collections4.MapUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DecoratorChain;
import com.latticeengines.domain.exposed.metadata.mds.MdsDecoratorFactory;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStore1;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.test.StepVerifier;

public class DecoratorChainUnitTestNG {

    private static final String K1 = "K1";
    private static final String K2 = "K2";
    private static final int MULTIPLIER = 1000;

    private static final MetadataStore1<String> feedMds = namespace -> {
        String key = namespace.getCoord1();
        if (K1.equals(key)) {
            return Flux.range(0, 20 * MULTIPLIER).map(DecoratorChainUnitTestNG::columnMetadata);
        } else {
            return null;
        }
    };

    private static final MetadataStore<Namespace1<String>> displayNameMds = (MetadataStore1<String>) namespace -> {
        String key = namespace.getCoord1();
        if (K2.equals(key)) {
            return Flux.range(5 * MULTIPLIER, 50 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.setDisplayName(displayName(k));
                return cm;
            });
        } else {
            return null;
        }
    };

    private static final MetadataStore<Namespace1<String>> descriptionNameMds = (MetadataStore1<String>) namespace -> {
        String key = namespace.getCoord1();
        if (K1.equals(key)) {
            return Flux.range(0, 10 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.setDescription(description(k));
                return cm;
            });
        } else {
            return null;
        }
    };

    private static final MetadataStore<Namespace1<String>> displayNameMds2 = (MetadataStore1<String>) namespace -> {
        String key = namespace.getCoord1();
        if (K1.equals(key)) {
            return Flux.range(0, 20 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.setDisplayName(displayName(2 * k));
                return cm;
            });
        } else {
            return null;
        }
    };

    private static final MetadataStore<Namespace1<String>> groupsMds = (MetadataStore1<String>) namespace -> {
        String key = namespace.getCoord1();
        if (K1.equals(key)) {
            return Flux.range(0, 20 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.enableGroup(ColumnSelection.Predefined.Enrichment);
                return cm;
            });
        } else {
            return null;
        }
    };

    private static final MetadataStore<Namespace1<String>> groupsMds2 = (MetadataStore1<String>) namespace -> {
        String key = namespace.getCoord1();
        if (K2.equals(key)) {
            Flux<ColumnMetadata> flux1 = Flux.range(0, 5 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                return cm;
            });
            Flux<ColumnMetadata> flux2 = Flux.range(5 * MULTIPLIER, 5 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.enableGroup(ColumnSelection.Predefined.ID);
                cm.enableGroup(ColumnSelection.Predefined.RTS);
                return cm;
            });
            Flux<ColumnMetadata> flux3 = Flux.range(10 * MULTIPLIER, 5 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.setGroups(null); // null will not overwrite
                return cm;
            });
            Flux<ColumnMetadata> flux4 = Flux.range(15 * MULTIPLIER, 5 * MULTIPLIER).map(k -> {
                ColumnMetadata cm = columnMetadata(k);
                cm.disableGroup(ColumnSelection.Predefined.Segment);
                return cm;
            });
            return flux1.concatWith(flux2).concatWith(flux3).concatWith(flux4);
        } else {
            return null;
        }
    };

    private static final Decorator D1 = MdsDecoratorFactory.fromMds("D1", displayNameMds).getDecorator(Namespace.as(K2));
    private static final Decorator D2 = MdsDecoratorFactory.fromMds("D2", descriptionNameMds).getDecorator(Namespace.as(K1));
    private static final Decorator D3 = MdsDecoratorFactory.fromMds("D3", displayNameMds2).getDecorator(Namespace.as(K1));
    private static final Decorator D4 = MdsDecoratorFactory.fromMds("D4", groupsMds).getDecorator(Namespace.as(K1));
    private static final Decorator D5 = MdsDecoratorFactory.fromMds("D5", groupsMds2).getDecorator(Namespace.as(K2));

    private static final DecoratorChain DC1 = new DecoratorChain("DC1", D1, D2);
    private static final DecoratorChain DC2 = new DecoratorChain("DC2", D3, DC1, D4, D5);

    @Test(groups = "unit")
    public void testFeedMds() {
        StepVerifier.FirstStep<ColumnMetadata> verifier = StepVerifier.create(feedMds.getMetadata(K1));
        for (int i = 0; i < 20 * MULTIPLIER; i++) {
            String attrName = attrName(i);
            verifier.consumeNextWith(cm -> {
                Assert.assertEquals(cm.getAttrName(), attrName);
                Assert.assertNull(cm.getDisplayName());
                Assert.assertNull(cm.getDescription());
                Assert.assertTrue(MapUtils.isEmpty(cm.getGroups()));
            });
        }
        verifier.verifyComplete();
    }

    @Test(groups = "unit")
    public  void testRenderDC1() {
        Flux<ColumnMetadata> flux = feedMds.getMetadata(K1);
        StepVerifier.FirstStep<ColumnMetadata> verifier = StepVerifier.create(DC1.render(flux));
        for (int i = 0; i < 20 * MULTIPLIER; i++) {
            String attrName = attrName(i);
            int idx = i;
            verifier.consumeNextWith(cm -> {
                Assert.assertEquals(cm.getAttrName(), attrName);
                // display name: 5..
                if (idx >= 5 * MULTIPLIER) {
                    Assert.assertEquals(cm.getDisplayName(), displayName(idx));
                } else {
                    Assert.assertNull(cm.getDisplayName());
                }
                // description: 0..9
                if (idx < 10 * MULTIPLIER) {
                    Assert.assertEquals(cm.getDescription(), description(idx));
                } else {
                    Assert.assertNull(cm.getDescription());
                }
                Assert.assertTrue(MapUtils.isEmpty(cm.getGroups()));
            });
        }
        verifier.verifyComplete();
    }

    @Test(groups = "unit")
    public  void testRenderDC2() {
        Flux<ColumnMetadata> flux = DC2.render(feedMds.getMetadata(K1));
        verifyRenderDC2(flux);
    }

    @Test(groups = "unit")
    public  void testRenderDC2Parallel() {
        ParallelFlux<ColumnMetadata> pflux = feedMds.getMetadata(K1) //
                .parallel().runOn(ThreadPoolUtils.getMdsScheduler());
        Flux<ColumnMetadata> flux = DC2.render(pflux)
                .sorted(Comparator.comparing(ColumnMetadata::getAttrName));
        verifyRenderDC2(flux);
    }

    private void verifyRenderDC2(Flux<ColumnMetadata> flux) {
        StepVerifier.FirstStep<ColumnMetadata> verifier = StepVerifier.create(flux);
        for (int i = 0; i < 20 * MULTIPLIER; i++) {
            String attrName = attrName(i);
            int idx = i;
            verifier.consumeNextWith(cm -> {
                try {
                    Assert.assertEquals(cm.getAttrName(), attrName);
                    // display name:
                    //   (0..4) -> 2 * k
                    //   (5..) -> k
                    if (idx < 5 * MULTIPLIER) {
                        Assert.assertEquals(cm.getDisplayName(), displayName(2 * idx));
                    } else {
                        Assert.assertEquals(cm.getDisplayName(), displayName(idx));
                    }
                    // groups:
                    //   (0..4) -> [-Enrichment]
                    //   (5..9) -> [ID, RTS, Enrichment],
                    //   (10..14) -> [Enrichment]
                    //   (15..) -> [Enrichment,-Segment]
                    if (idx < 5 * MULTIPLIER) {
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.RTS));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.ID));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.Enrichment));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.Segment));
                    } else if (idx < 10 * MULTIPLIER) {
                        Assert.assertTrue(cm.isEnabledFor(ColumnSelection.Predefined.RTS));
                        Assert.assertTrue(cm.isEnabledFor(ColumnSelection.Predefined.ID));
                        Assert.assertTrue(cm.isEnabledFor(ColumnSelection.Predefined.Enrichment));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.Segment));
                    } else if (idx < 15 * MULTIPLIER) {
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.RTS));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.ID));
                        Assert.assertTrue(cm.isEnabledFor(ColumnSelection.Predefined.Enrichment));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.Segment));
                    } else {
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.RTS));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.ID));
                        Assert.assertTrue(cm.isEnabledFor(ColumnSelection.Predefined.Enrichment));
                        Assert.assertFalse(cm.isEnabledFor(ColumnSelection.Predefined.Segment));
                    }
                } catch (AssertionError e) {
                    Assert.fail(JsonUtils.serialize(cm), e);
                }
            });
        }
        verifier.verifyComplete();
    }

    private static ColumnMetadata columnMetadata(int idx) {
        ColumnMetadata cm = new ColumnMetadata();
        cm.setAttrName(attrName(idx));
        return cm;
    }

    private static String attrName(int idx) {
        int ndigit = (int) Math.log10(MULTIPLIER);
        return String.format("Attr%0" + (ndigit + 2) + "d", idx);
    }

    private static String displayName(int idx) {
        return String.format("Attribute %d", idx);
    }

    private static String description(int idx) {
        return String.format("A testing attribute with id %d", idx);
    }

}
