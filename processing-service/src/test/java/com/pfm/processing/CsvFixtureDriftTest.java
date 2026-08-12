package com.pfm.processing;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The golden CSV fixture is committed twice: the classpath copy that
 * {@link FullPipelineGoldenTest} asserts against
 * (src/test/resources/Output.csv), and the shipped artifact at
 * sample-output/Output.csv. Nothing else checks that the two stay in sync, so
 * regenerating one and forgetting the other would leave the golden test
 * passing against a stale twin while silently voiding the project's central
 * contract.
 */
class CsvFixtureDriftTest {

    @Test
    void classpathFixtureMatchesShippedSampleOutput() throws IOException {
        byte[] classpathCopy;
        try (InputStream in = CsvFixtureDriftTest.class.getResourceAsStream("/Output.csv")) {
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
