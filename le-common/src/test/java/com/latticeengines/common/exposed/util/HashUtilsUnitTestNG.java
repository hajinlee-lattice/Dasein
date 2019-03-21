package com.latticeengines.common.exposed.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import reactor.core.publisher.Flux;

public class HashUtilsUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(HashUtilsUnitTestNG.class);

    private static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final int NUM_SEEDS = 10;
    private static final int SEED_LENGTH = 16;
    private static final int MAX_LOWER_CASES = 8;
    private static final int MAX_SPACES = 1;
    private static final int MAX_UNDERSCORES = 1;

    private Random random;

    @Test(groups = "unit")
    public void testHashCollision() {
        random = new Random(System.currentTimeMillis());
        List<String> seeds = Flux.range(0, NUM_SEEDS).map(k -> getRandomString(SEED_LENGTH)).collectList().block();
        log.info("Generating distinct tokens from seeds ...");
        List<String> tokens = generateTokens(seeds).collectList().block();
        Assert.assertNotNull(tokens);
        log.info("Generated " + tokens.size() + " distinct strings");
        List<String> hashs = Flux.fromIterable(tokens).parallel().map(HashUtils::getShortHash).sequential().distinct().collectList().block();
        Assert.assertNotNull(hashs);
        long distinctHashs = hashs.size();
        long collidedHashs = tokens.size() - distinctHashs;
        double collisionRate = 1.0 * collidedHashs / tokens.size();
        log.info("Converted into " + distinctHashs + " distinct hashs, " + collidedHashs + " are duplicated, collision rate ~ " + collisionRate);
        Assert.assertTrue(collisionRate < 0.01, "Should only have < 1% collisions");

        Integer maxLength = hashs.stream().map(String::length).max(Comparator.naturalOrder()).orElse(null);
        Assert.assertNotNull(maxLength);
        log.info("Maximum length of hash is " + maxLength);
    }

    @Test(groups = "unit")
    public void testCleanedString() {
        String hashed = HashUtils.getShortHash("a product name supposed to be hashed.");
        log.info("Hashed string: " + hashed);
        Assert.assertTrue(HashUtils.getCleanedString(hashed).matches("[a-zA-Z0-9]*"));
        Assert.assertFalse("abcd_1234/345!gdas?dsgafasd/dasgdsa\\dagdas".matches("[a-zA-Z0-9]*"));
        Assert.assertTrue(HashUtils.getCleanedString("abcd_1234/345!gdas?dsgafasd/dasgdsa\\dagdas")
                .matches("[a-zA-Z0-9]*"));
    }

    @Test(groups = "unit")
    public void testSameHashValue() {
        Assert.assertEquals(HashUtils.getShortHash(HashUtils.getCleanedString(LETTERS)),
                HashUtils.getShortHash(HashUtils.getCleanedString(LETTERS)));
        Assert.assertNotEquals(HashUtils.getShortHash(HashUtils.getCleanedString("abcd")),
                HashUtils.getShortHash(HashUtils.getCleanedString("abcc")));
    }

    private String getRandomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(LETTERS.charAt(random.nextInt(26)));
        }
        return sb.toString();
    }


    private Flux<String> generateTokens(Collection<String> seeds) {
        Flux<String> tokens = Flux.fromIterable(seeds);
        for (int i = 0; i < Math.min(MAX_LOWER_CASES, SEED_LENGTH); i++) {
            tokens = changeOneCharToLowerCase(tokens);
        }
        for (int i = 0; i < MAX_SPACES; i++) {
            tokens = tokens.concatWith(addOneSpace(tokens));
        }
        for (int i = 0; i < MAX_UNDERSCORES; i++) {
            tokens = tokens.concatWith(addOneUnderscore(tokens));
        }
        return tokens.distinct();
    }

    private Flux<String> changeOneCharToLowerCase(Flux<String> seeds) {
        return seeds.parallel(4).flatMap(s -> {
            List<String> generated = new ArrayList<>();
            generated.add(s);
            for (int i = 0; i < s.length(); i++) {
                StringBuilder sb = new StringBuilder();
                if (i  > 0) {
                    sb.append(s.substring(0, i));
                }
                sb.append(String.valueOf(s.charAt(i)).toLowerCase());
                if (i < s.length() - 1) {
                    sb.append(s.substring(i + 1, s.length()));
                }
                generated.add(sb.toString());
            }
            return Flux.fromIterable(generated);
        }).sequential().distinct();
    }

    private Flux<String> addOneSpace(Flux<String> seeds) {
        return seeds.parallel(4).map(s -> {
            int pos = random.nextInt(s.length());
            StringBuilder sb = new StringBuilder();
            if (pos == 0) {
                sb.append(" ");
                sb.append(s);
            } else if (pos == s.length() - 1) {
                sb.append(s);
                sb.append(" ");
            } else {
                sb.append(s.substring(0, pos));
                sb.append(" ");
                sb.append(s.substring(pos));
            }
            return sb.toString();
        }).sequential().distinct();
    }

    private Flux<String> addOneUnderscore(Flux<String> seeds) {
        return seeds.parallel(4).map(s -> {
            int pos = random.nextInt(s.length());
            StringBuilder sb = new StringBuilder();
            if (pos == 0) {
                sb.append("_");
                sb.append(s);
            } else if (pos == s.length() - 1) {
                sb.append(s);
                sb.append("_");
            } else {
                sb.append(s.substring(0, pos));
                sb.append("_");
                sb.append(s.substring(pos));
            }
            return sb.toString();
        }).sequential().distinct();
    }

}
