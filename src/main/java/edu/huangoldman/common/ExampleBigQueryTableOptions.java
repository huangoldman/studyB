package edu.huangoldman.common;

import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExampleBigQueryTableOptions extends GcpOptions {

    @Description("BigQuery dataset name")
    @Default.String("beam例子")
    String getBigQueryDataSet();

    void setBigQueryDataSet(String dataSet);

    @Description("BigQuery table name")
    @Default.InstanceFactory(BigQueryTableFactory.class)
    String getBigQueryTable();

    void setBigQueryTable(String table);

    @Description("BigQuery table schema")
    TableSchema getBigQuerySchema();

    void setBigQuerySchema(TableSchema schema);

    class BigQueryTableFactory implements DefaultValueFactory<String> {

        @Override
        public String create(PipelineOptions options) {
            return options.getJobName().replace('-', '_');
        }

    }
}
