package edu.huangoldman.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class DebuggingWordCount {

    static void runDebuggingWordCount(WordCountOptions options) {

        Pipeline p = Pipeline.create(options);
        PCollection<KV<String, Long>> filteredWords =
                p.apply("FormatAsTextFnReadLines", TextIO.read().from(options.getInputFile()))
                        .apply(new WordCount.CountWords())
                        .apply(ParDo.of(new FilterTextFn(options.getFilterPattern())));

        List<KV<String, Long>> expectedResuts = Arrays.asList(
                KV.of("can", 1L),
                KV.of("will", 1L)
        );

        PAssert.that(filteredWords).containsInAnyOrder(expectedResuts);

        p.run().waitUntilFinish();

    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountOptions.class);

        runDebuggingWordCount(options);

    }

    public interface WordCountOptions extends WordCount.WordCountOptions {

        @Description("单词过滤，仅仅只有匹配模式的单次才会被计数")
        @Default.String("can|will")
        String getFilterPattern();


        void setFilterPattern(String Value);
    }

    public static class FilterTextFn extends DoFn<KV<String, Long>, KV<String, Long>> {
        private static final Logger LOG = LoggerFactory.getLogger(FilterTextFn.class);

        private final Pattern filter;
        private final Counter matchedWords = Metrics.counter(FilterTextFn.class, "matchedWords");
        private final Counter unmatchedWords = Metrics.counter(FilterTextFn.class, "unmatchedWords");

        public FilterTextFn(String pattern) {
            filter = Pattern.compile(pattern);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            if (filter.matcher(c.element().getKey()).matches()) {
                LOG.debug("Matched : " + c.element().getKey());
                matchedWords.inc();
                c.output(c.element());
            } else {
                LOG.debug("Did not match :" + c.element().getKey());
                unmatchedWords.inc();
            }
        }
    }

}
