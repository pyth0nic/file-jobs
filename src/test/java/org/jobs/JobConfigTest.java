package org.jobs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobConfigTest {
    @Test
    void parsesRequiredAndOptionalArguments() {
        JobConfig config = JobConfig.parse(new String[] {
                "--input", "s3://bucket/a.zip,s3://bucket/b.zip", "--output", "s3://bucket/output",
                "--checkpoint-dir", "s3://bucket/checkpoints", "--parallelism", "2", "--filter", "match"
        });
        assertEquals(2, config.inputs.size());
        assertEquals(2, config.parallelism);
        assertEquals("match", config.filter);
    }

    @Test
    void rejectsMissingRequiredArgumentsAndInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(new String[] {"--input", "a.zip"}));
        assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(new String[] {
                "--input", "a.zip", "--output", "out", "--checkpoint-dir", "checkpoints", "--parallelism", "0"
        }));
        assertThrows(IllegalArgumentException.class, () -> JobConfig.parse(new String[] {
                "--input", "a.zip", "--output", "out", "--checkpoint-dir", "checkpoints", "--unknown", "value"
        }));
    }
}
