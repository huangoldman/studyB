package edu.huangoldman.game;

import edu.huangoldman.common.ExampleOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.joda.time.Duration;

public class LeaderBoard extends HourlyTeamScore {

    static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
    static final Duration TEN_MINUTES = Duration.standardMinutes(10);

    interface Options extends HourlyTeamScore.Options, ExampleOptions, StreamingOptions {
        @Description("BigQuery Dataset to write tables to. Must already exist.")
        @Validation.Required
        String getDataset();

        void setDataset(String value);

        @Description("Pub/Sub topic to read from")
        @Validation.Required
        String getTopic();

        void setTopic(String value);

        @Description("Numeric value of fixed window duration for team analysis, in minutes")
        @Default.Integer(60)
        Integer getTeamWindowDuration();

        void setTeamWindowDuration(Integer value);

        @Description("Numeric value of allowed data lateness, in minutes")
        @Default.Integer(120)
        Integer getAllowedLateness();

        void setAllowedLateness(Integer value);

        @Description("Prefix used for the BigQuery table names")
        @Default.String("leaderboard")
        String getLeaderBoardTableName();

        void setLeaderBoardTableName(String value);
    }


}

