package com.pfm.processing;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@link FullPipelineGoldenTest} proves the pipeline turns one specific input into one
 * specific output, and both of those files are committed more than once: on the test
 * classpath (what the golden test actually uses) and as the shipped artifacts under
 * sample-data/ and sample-output/ (what a reviewer runs and reads).
 *
 * <p>Nothing else checks that the copies stay in sync, so regenerating or replacing one
 * and forgetting the other leaves the golden test passing against a stale twin while
 * silently voiding the project's central contract.
 */
class FixtureDriftTest {

    @Test
    void classpathInputFixtureMatchesTheIngestedSampleData() throws IOException {
        // Input.txt is committed four times: sample-data/ (what compose and scripts/run.sh
        // actually ingest, and what the README invites reviewers to replace) plus a classpath
        // copy in each of the three modules. Output.csv gets a drift guard for exactly this
        // reason; the input side had the same silent-rot failure mode and no guard, so editing
        // sample-data/Input.txt left `mvn verify` green against three unchanged copies while
        // voiding the contract the golden test claims to prove.
        byte[] classpathCopy;
        try (InputStream in = FixtureDriftTest.class.getResourceAsStream("/Input.txt")) {
            if (in == null) {
                throw new IllegalStateException("Missing test fixture /Input.txt on the classpath");
            }
            classpathCopy = in.readAllBytes();
        }

        Path shipped = Path.of("../sample-data/Input.txt");
        if (!Files.exists(shipped)) {
            fail("Cannot find " + shipped.toAbsolutePath()
                    + " -- expected Surefire's working directory to be the processing-service module base dir");
        }

        assertArrayEquals(classpathCopy, Files.readAllBytes(shipped),
                "processing-service/src/test/resources/Input.txt has drifted from sample-data/Input.txt. "
                        + "The golden test asserts against the classpath copy, so the shipped sample and the "
                        + "proven-correct input are no longer the same file. Re-sync them (note common/ and "
                        + "ingestion-service/ carry copies too).");
    }

    @Test
    void classpathFixtureMatchesShippedSampleOutput() throws IOException {
        byte[] classpathCopy;
        try (InputStream in = FixtureDriftTest.class.getResourceAsStream("/Output.csv")) {
            if (in == null) {
                throw new IllegalStateException("Missing test fixture /Output.csv on the classpath");
            }
            classpathCopy = in.readAllBytes();
        }

        // Surefire's working directory is the module base dir (processing-service/), so this
        // relative path resolves to <repo-root>/sample-output/Output.csv.
        Path shipped = Path.of("../sample-output/Output.csv");
        if (!Files.exists(shipped)) {
            fail("Cannot find " + shipped.toAbsolutePath()
                    + " -- expected Surefire's working directory to be the processing-service module base dir");
        }
        byte[] shippedCopy = Files.readAllBytes(shipped);

        assertArrayEquals(classpathCopy, shippedCopy,
                "processing-service/src/test/resources/Output.csv (authoritative -- this is what "
                        + "FullPipelineGoldenTest asserts against) has drifted from sample-output/Output.csv "
                        + "(the shipped artifact). Regenerate sample-output/Output.csv from the classpath "
                        + "copy so the two stay byte-identical.");
    }
}
