package edu.huangoldman.common;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;


public class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {

    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

    private String filenamPrefix;

    @Nullable
    private Integer numShards;

    public WriteOneFilePerWindow(String filenamPrefix, Integer numShards) {
        this.filenamPrefix = filenamPrefix;
        this.numShards = numShards;
    }

    @Override
    public PDone expand(PCollection<String> input) {

        return null;
    }
}
