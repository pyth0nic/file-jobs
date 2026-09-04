package org.jobs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class CsvRecordProcessor extends ProcessFunction<ZipCsvSource.ZipLine, GenericRecord> {
    static final OutputTag<String> REJECTED_RECORDS = new OutputTag<String>("rejected-records") { };
    private final String filter;
    private transient Schema schema;
    private transient Counter acceptedRecords;
    private transient Counter rejectedRecords;

    CsvRecordProcessor(String filter) {
        this.filter = filter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        schema = new Schema.Parser().parse(Objects.requireNonNull(
                CsvRecordProcessor.class.getResourceAsStream("/schema.avsc"), "Missing resource: /schema.avsc"));
        acceptedRecords = getRuntimeContext().getMetricGroup().counter("acceptedRecords");
        rejectedRecords = getRuntimeContext().getMetricGroup().counter("rejectedRecords");
    }

    @Override
    public void processElement(ZipCsvSource.ZipLine input, Context context, Collector<GenericRecord> output) {
        try {
            List<String> fields = parse(input.line);
            if (fields.size() != schema.getFields().size()) {
                throw new IllegalArgumentException("expected " + schema.getFields().size() + " fields but found " + fields.size());
            }
            if (!filter.isEmpty() && !input.line.contains(filter)) return;
            GenericRecord record = new GenericData.Record(schema);
            for (int index = 0; index < fields.size(); index++) record.put(index, fields.get(index));
            acceptedRecords.inc();
            output.collect(record);
        } catch (IllegalArgumentException exception) {
            rejectedRecords.inc();
            context.output(REJECTED_RECORDS, csvEscape(input.archive) + "," + csvEscape(input.entry) + "," + input.lineNumber + ","
                    + csvEscape(exception.getMessage()) + "," + csvEscape(input.line));
        }
    }

    static List<String> parse(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean afterQuote = false;
        for (int index = 0; index < line.length(); index++) {
            char character = line.charAt(index);
            if (quoted) {
                if (character == '"') {
                    if (index + 1 < line.length() && line.charAt(index + 1) == '"') {
                        field.append('"');
                        index++;
                    } else {
                        quoted = false;
                        afterQuote = true;
                    }
                } else {
                    field.append(character);
                }
            } else if (character == ',') {
                fields.add(field.toString());
                field.setLength(0);
                afterQuote = false;
            } else if (character == '"' && field.length() == 0 && !afterQuote) {
                quoted = true;
            } else if (afterQuote) {
                throw new IllegalArgumentException("unexpected character after closing quote");
            } else {
                field.append(character);
            }
        }
        if (quoted) throw new IllegalArgumentException("unterminated quoted field");
        fields.add(field.toString());
        return fields;
    }

    static String csvEscape(String value) {
        return "\"" + (value == null ? "" : value.replace("\"", "\"\"")) + "\"";
    }
}
