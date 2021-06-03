package org.jobs;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;

public class CSV implements IndexedRecord {
    private final String line;

    private static Schema schema;

    public CSV(String line) {
        this.line = line;
    }

    static {
        try {
            schema = new Schema.Parser().parse(CSV.class.getClassLoader().getResourceAsStream(FileUtils.SCHEMA_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void put(int i, Object o) {
        throw new NotImplementedException("Not needed");
    }

    @Override
    public Object get(int i) {
        return line;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }
}
