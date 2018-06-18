package edu.huangoldman.game.utils;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class WriteToText<InputT>
    extends PTransform<PCollection<InputT> , PDone> {

    private static final DateTimeFormatter formatter =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .withZone(DateTimeZone.forTimeZone(
                        TimeZone.getTimeZone("GTM+8")
                ));

    protected String filenamePrefix;

    protected Map<String,FieldFn<InputT>> fieldFn;

    protected boolean windowed;

    public WriteToText(){

    }

    public WriteToText(String filenamePrefix,
                       Map<String,FieldFn<InputT>> fieldFn,
                       boolean windowed){
        this.filenamePrefix = filenamePrefix;
        this.fieldFn = fieldFn;
        this.windowed = windowed;
    }

    public interface FieldFn<InputT> extends Serializable{
        Object apply(DoFn<InputT, String>.ProcessContext context, BoundedWindow window);    }

    protected class BuildRowFn extends DoFn<InputT , String>{

        @ProcessElement
        public void processElement(ProcessContext c,BoundedWindow window){
            List<String> fields = new ArrayList<>();
            for(Map.Entry<String,FieldFn<InputT>> entry : fieldFn.entrySet()){
                fields.add(entry.getKey() + ": " + entry.getValue().apply(c,window));
            }
            String result = fields.stream().collect(Collectors.joining(", "));
            c.output(result);
        }
    }

    protected class WriteOneFilePerWindow extends PTransform<PCollection<String>,PDone>{

        private final String filenamePrefix;

        public WriteOneFilePerWindow(String filenamePrefix){
            this.filenamePrefix = filenamePrefix;
        }

        @Override
        public PDone expand(PCollection<String> input){
            ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);

            return input.apply(
                    TextIO.write()
                            .to(new PerWindowFiles(resource))
                            .withTempDirectory(resource.getCurrentDirectory())
                            .withWindowedWrites()
                            .withNumShards(3));
        }
    }

    protected static class PerWindowFiles extends FilenamePolicy {

        private final ResourceId prefix;

        public PerWindowFiles(ResourceId prefix) {
            this.prefix = prefix;
        }

        public String filenamePrefixForWindow(IntervalWindow window) {
            String filePrefix = prefix.isDirectory() ? "" : prefix.getFilename();
            return String.format(
                    "%s-%s-%s", filePrefix, formatter.print(window.start()), formatter.print(window.end()));
        }

        @Override
        public ResourceId windowedFilename(int shardNumber,
                                           int numShards,
                                           BoundedWindow window,
                                           PaneInfo paneInfo,
                                           OutputFileHints outputFileHints) {
            IntervalWindow intervalWindow = (IntervalWindow) window;
            String filename =
                    String.format(
                            "%s-%s-of-%s%s",
                            filenamePrefixForWindow(intervalWindow),
                            shardNumber,
                            numShards,
                            outputFileHints.getSuggestedFilenameSuffix());
            return prefix.getCurrentDirectory().resolve(filename, StandardResolveOptions.RESOLVE_FILE);
        }

        @Override
        public ResourceId unwindowedFilename(
                int shardNumber, int numShards, OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("Unsupported.");
        }
    }

    public PDone expand(PCollection<InputT> teamAndScore){
        if(windowed){
            teamAndScore
                    .apply("ConvertToRow",ParDo.of(new BuildRowFn()))
                    .apply(new WriteToText.WriteOneFilePerWindow(filenamePrefix));

        }else {
            teamAndScore
                    .apply("ConvertToRow",ParDo.of(new BuildRowFn()))
                    .apply(TextIO.write().to(filenamePrefix));
        }
        return PDone.in(teamAndScore.getPipeline());
    }
}
