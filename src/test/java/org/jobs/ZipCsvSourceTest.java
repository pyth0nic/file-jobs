package org.jobs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZipCsvSourceTest {
    @Test
    void acceptsOnlySafeCsvEntries() {
        assertTrue(ZipCsvSource.isCsvEntry("nested/data.CSV"));
        assertFalse(ZipCsvSource.isCsvEntry("../data.csv"));
        assertFalse(ZipCsvSource.isCsvEntry("/data.csv"));
        assertFalse(ZipCsvSource.isCsvEntry("\\\\server\\data.csv"));
        assertFalse(ZipCsvSource.isCsvEntry("C:\\data.csv"));
        assertFalse(ZipCsvSource.isCsvEntry("data.txt"));
    }
}
