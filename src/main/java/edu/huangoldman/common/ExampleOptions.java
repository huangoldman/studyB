package edu.huangoldman.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface ExampleOptions extends PipelineOptions {

    @Description("是否再本地进程退出后，保持模式的运行")
    @Default.Boolean(false)
    boolean getKeepJobsRunning();

    void setKeepJobsRunning(boolean keepJobsRunning);

    @Description("执行管道时要使用的worker的数量")
    @Default.Integer(1)
    int getInjectorNumWorkers();

    void setInjectorNumWorkers(int numWorkers);

}

