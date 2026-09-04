package org.jobs;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CsvRecordProcessorTest {
    @Test
    void parsesQuotedCsvFields() {
        assertEquals(List.of("a,b", "quoted \"value\"", ""), CsvRecordProcessor.parse("\"a,b\",\"quoted \"\"value\"\"\","));
    }

    @Test
    void rejectsUnterminatedQuotedField() {
        assertThrows(IllegalArgumentException.class, () -> CsvRecordProcessor.parse("\"unterminated"));
    }
}
