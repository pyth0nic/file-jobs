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

    @Test
    void prefersLaterEntryProgressOverEarlierLineNumbers() {
        ZipCsvSource.ArchiveProgress earlier = ZipCsvSource.ArchiveProgress.at("archive.zip", "a.csv", 0, 100);
        ZipCsvSource.ArchiveProgress later = ZipCsvSource.ArchiveProgress.at("archive.zip", "b.csv", 1, 1);
        assertTrue(later.isLaterThan(earlier));
        assertFalse(earlier.isLaterThan(later));
    }
}
