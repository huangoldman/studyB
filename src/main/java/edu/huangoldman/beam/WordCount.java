package edu.huangoldman.beam;

import edu.huangoldman.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WordCount {

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(WordCountOptions.class);

        runWordCount(options);

    }

    static void runWordCount(WordCountOptions options) {
        Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public interface WordCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("pom.xml")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Required
        @Default.String("count")
        String getOutput();

        void setOutput(String value);
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ":" + input.getValue();
        }
    }

    public static class CountWords extends PTransform<PCollection<String>,
            PCollection<KV<String, Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            PCollection<String> words = lines.apply(
                    ParDo.of(new ExtractWordsFn())
            );
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());
            return wordCounts;
        }
    }

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");

        private final Distribution lineLenDist = Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(ProcessContext c) {
            lineLenDist.update(c.element().length());
            if (c.element().trim().isEmpty()) {
                emptyLines.inc();
            }
            String[] words = c.element().split(ExampleUtils.TOKENIZER_PATTERN);

            for (String word : words) {
                if (!word.isEmpty()) {
                    c.output(word);
                }
            }

        }
    }


}
