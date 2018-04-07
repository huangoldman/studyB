package edu.huangoldman.beam;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class MinimalWordCount {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        p.apply(TextIO.read().from("pom.xml"))
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                        .via((String word) -> Arrays.asList(word.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()))
                .apply(Count.perElement())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ":" + wordCount.getValue()))
                .apply(TextIO.write().to("counts1"));

        p.run().waitUntilFinish();


    }

}
