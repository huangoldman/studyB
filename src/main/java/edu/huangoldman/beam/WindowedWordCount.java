package edu.huangoldman.beam;

import edu.huangoldman.common.ExampleBigQueryTableOptions;
import edu.huangoldman.common.ExampleOptions;
import edu.huangoldman.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;


public class WindowedWordCount {
    static final int WINDOW_SIZE = 10;

    static void runWindowedWordCount(Options options) throws IOException {
        final String output = options.getOutput();
        final Instant minTimeStamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimeStamp = new Instant(options.getMaxTimestampMillis());

        Pipeline pipeline = Pipeline.create(options);

        //Beam可以让我们无论是有界或无界输入源都运行相同的管道
        PCollection<String> input = pipeline
                .apply(TextIO.read().from(options.getInputFile()))
                //给每个元素添加一个人工的时间戳
                .apply(ParDo.of(new AddTimestampFn(minTimeStamp, maxTimeStamp)));


        PCollection<String> windowedWords =
                input.apply(
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize())))
                );

        PCollection<KV<String, Long>> wordCounts = windowedWords.apply(
                new WordCount.CountWords()
        );

        wordCounts
                .apply(MapElements.via(new WordCount.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(null, null));


    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    }

    public interface Options extends WordCount.WordCountOptions, ExampleOptions,
            ExampleBigQueryTableOptions {

        @Description("固定的时间窗口，按分钟算")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("最小随机分配时间戳，以毫秒为单位")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("最大随机分配时间戳，以毫秒位单位")
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("每个窗口生成的固定的数量")
        Integer getNumShards();

        void setNumShards(Integer numShards);

    }

    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }

    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(Options.class).getMinTimestampMillis()
                    + Duration.standardHours(1).getMillis();
        }
    }

    static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant randomTimestamp =
                    new Instant(
                            ThreadLocalRandom.current()
                                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis())
                    );
            c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
            System.out.print("test git");

        }
    }
}



