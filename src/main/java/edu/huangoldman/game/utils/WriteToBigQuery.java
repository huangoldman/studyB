package edu.huangoldman.game.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WriteToBigQuery<InputT>
        extends PTransform<PCollection<InputT>, PDone> {

    protected String projectId;
    protected String datasetId;
    protected String tableName;
    protected Map<String, FieldInfo<InputT>> fieldInfo;

    public WriteToBigQuery() {
    }

    public WriteToBigQuery(
            String projectId,
            String datasetId,
            String tableName,
            Map<String, FieldInfo<InputT>> fieldInfo) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableName = tableName;
        this.fieldInfo = fieldInfo;
    }


    public interface FieldFn<InputT> extends Serializable {
        Object apply(DoFn<InputT, TableRow>.ProcessContext context, BoundedWindow window);

    }

    //定义一个类来保存有关输出表字段定义的信息
    public static class FieldInfo<InputT> implements Serializable {

        private String fieldType;

        private FieldFn<InputT> fieldFn;

        public FieldInfo(String fieldType,
                         FieldFn<InputT> fieldFn) {
            this.fieldType = fieldType;
            this.fieldFn = fieldFn;
        }

        String getFieldType() {
            return this.fieldType;
        }

        FieldFn<InputT> getFieldFn() {
            return this.fieldFn;
        }
    }

    //按照fieldFn的描述，将每一个键值对转换成一个BigQuery TableRow
    protected class BuildRowFn extends DoFn<InputT, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext context,BoundedWindow window){
            TableRow row = new TableRow();
            for(Map.Entry<String,FieldInfo<InputT>> entry : fieldInfo.entrySet()){
                String key = entry.getKey();
                FieldInfo<InputT> fcnInfo = entry.getValue();
                FieldFn<InputT> fcn = fcnInfo.getFieldFn();
                row.set(key,fcn.apply(context,window));
            }
            context.output(row);
        }
    }

    //构建输出表的格式
    protected TableSchema getSchema(){
        List<TableFieldSchema> fields = new ArrayList<>();
        for(Map.Entry<String,FieldInfo<InputT>> entry : fieldInfo.entrySet()){
            String key = entry.getKey();
            FieldInfo<InputT> fcnInfo = entry.getValue();
            String bqType = fcnInfo.getFieldType();
            fields.add(new TableFieldSchema().setName(key).setType(bqType));
        }
        return new TableSchema().setFields(fields);
    }

    //构建输出表引用的实用工具
    static TableReference getTable(String projectId,String datasetId,String tableName){
        TableReference reference = new TableReference();
        reference.setProjectId(projectId);
        reference.setDatasetId(datasetId);
        reference.setTableId(tableName);
        return reference;
    }

    @Override
    public PDone expand(PCollection<InputT> teamAndScore) {

        teamAndScore.apply("ConvertToRow",ParDo.of(new BuildRowFn()))
                    .apply(BigQueryIO.writeTableRows()
                                .to(getTable(projectId, datasetId, tableName))
                                .withSchema(getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        return PDone.in(teamAndScore.getPipeline());

    }


}
