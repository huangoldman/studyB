package edu.huangoldman.game;


import com.oracle.tools.packager.Log;
import edu.huangoldman.game.utils.WriteToText;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class UserScore {

    @DefaultCoder(AvroCoder.class)
    static class GameActionInfo {
        @Nullable String user;
        @Nullable String team;
        @Nullable Integer score;
        @Nullable Long timestamp;

        public GameActionInfo() {}

        public GameActionInfo(String user, String team, Integer score, Long timestamp) {
            this.user = user;
            this.team = team;
            this.score = score;
            this.timestamp = timestamp;
        }

        public String getUser() {
            return this.user;
        }
        public String getTeam() {
            return this.team;
        }
        public Integer getScore() {
            return this.score;
        }
        public String getKey(String keyname) {
            if (keyname.equals("team")) {
                return this.team;
            } else {  // return username as default
                return this.user;
            }
        }
        public Long getTimestamp() {
            return this.timestamp;
        }
    }

    static class ParseEventFn extends DoFn<String,GameActionInfo>{
        private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
        private final Counter numParseErros = Metrics.counter("main","ParseErrors");

        @ProcessElement
        public void processElement(ProcessContext c){
            String[] components = c.element().split(",");
            try{
                GameActionInfo gInfo = new GameActionInfo(components[0].trim(),components[1].trim(),
                        Integer.parseInt(components[2].trim()),Long.parseLong(components[3].trim()));
                c.output(gInfo);
            }catch (ArrayIndexOutOfBoundsException | NumberFormatException e){
                numParseErros.inc();
                Log.info("Parse error on " + c.element() + "," + e.getMessage());
            }
        }
    }

    public static class ExtractAndSumScore extends
            PTransform<PCollection<GameActionInfo> , PCollection<KV<String,Integer>>>{

        private final String field;

        ExtractAndSumScore(String field){
            this.field = field;
        }

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameInfo) {

            return gameInfo.apply(
                    MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                            TypeDescriptors.integers()))
                            .via( (GameActionInfo gInfo) -> KV.of(gInfo.getKey(field) , gInfo.getScore()))
                    ).apply(Sum.integersPerKey());
        }
    }

    public interface Options extends PipelineOptions{

        @Description("game data")
        @Default.String("input.csv")
        String getInput();
        void setInput(String value);

        @Description("output")
        @Validation.Required
        @Default.String("score_output")
        String getOutput();
        void setOutput(String value);
    }

    protected static Map<String,WriteToText.FieldFn<KV<String,Integer>>>
        configueOutput(){
        Map<String, WriteToText.FieldFn<KV<String, Integer>>> config = new HashMap<>();
        config.put("user",(c,w) -> c.element().getKey());
        config.put("total_score",(c,w) -> c.element().getValue());
        return config;
    }

    public static void main(String[] args) throws Exception{

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInput()))
                .apply("ParseGameEvent",ParDo.of(new ParseEventFn()))
                .apply("ExtractUserScore",new ExtractAndSumScore("user"))
                .apply("WriteUserScoreSums",new WriteToText<>(options.getOutput(),configueOutput(),false));

        pipeline.run().waitUntilFinish();
    }




}
