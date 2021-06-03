package org.jobs;

import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

public class FileSearchFunctions {
    public static DataSet<Tuple2<Void, IndexedRecord>> processStream(DataSet<String> stringStream) {
        DataSet<Tuple2<Void, IndexedRecord>> recordStream = stringStream
                .filter(FileSearchFunctions.getEllipsis())
                .map(FileSearchFunctions.convertToIndexedRecord());
        return recordStream;
    }

    public static FilterFunction<String> getEllipsis() {
        return x -> x.contains("ellipsis");
    }

    public static MapFunction<String, Tuple2<Void, IndexedRecord>> convertToIndexedRecord() {
        return new MapFunction<String, Tuple2<Void, IndexedRecord>>() {
            @Override
            public Tuple2<Void, IndexedRecord> map(String x) throws Exception {
                return Tuple2.of(null, new CSV(x));
            }
        };
    }
}
