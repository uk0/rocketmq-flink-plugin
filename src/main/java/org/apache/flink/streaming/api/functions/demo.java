package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.HdfsSink;
import org.apache.flink.streaming.api.functions.source.RocketMQSource;

/**
 * Created by zhangjianxin on 2017/11/29.
 * Github Breakeval13
 * blog firsh.me
 */
public class demo
{
    /**
     * org.apache.flink.streaming.api.functions.demo
     *
     * @param args 参数样例：
     *             --topic data --cgroup data_group
     *             --sourceParallelism 2
     *             --hdfsSinkParallelism 10
     *             --sinkParallelism  10
     *             --mapParallelism 10
     *             --keybyms 60000
     *             --charset UTF-8
     *             --confdir /home/flow/DAT/temp
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 解析参数

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        int argLength = 7;
        if (parameterTool.getNumberOfParameters() < argLength) {
            System.out.println("Missing parameters!");
            System.out.println("\nUsage: GlobleConfig  as ExpressStatusFlow\n" +
                    "--topic <topic> \n" +
                    "--cgroup <consumerGroupName> \n" +
                    "--sourceParallelism <sourceParallelism default 1> \n" +
                    "--sinkParallelism <sinkParallelism default 1> \n" +
                    "--mapParallelism <mapParallelism default 1> \n" +
                    "--hdfsSinkParallelism <hdfsSinkParallelism default 1> \n" +
                    "--keybyms <keybyms default 100> \n" +
                    "--charset <CharsetName Messsage> \n" +
                    "--confdir <config file path for All NODE > \n"
            );
            return;
        }

        String topic = parameterTool.get("topic");
        int mapParallelism = parameterTool.getInt("mapParallelism");
        int sourceParallelism = parameterTool.getInt("sourceParallelism");
        int sinkParallelism = parameterTool.getInt("sinkParallelism");
        int hdfsSinkParallelism = parameterTool.getInt("hdfsSinkParallelism");
        int keyByMilliseconds = parameterTool.getInt("keybyms");

        env.getConfig().enableSysoutLogging();
        Configuration conf = new Configuration();
        conf.setString("topic", topic);
        conf.setString("charset", parameterTool.get("charset"));
        conf.setString("cgroup", parameterTool.get("cgroup"));
        conf.setString("confdir", parameterTool.get("confdir"));
        conf.setInteger("sourceParallelism", sourceParallelism);
        conf.setInteger("sinkParallelism", sinkParallelism);
        conf.setInteger("mapParallelism", mapParallelism);
        conf.setInteger("hdfsSinkParallelism", hdfsSinkParallelism);
        conf.setInteger("keybyms", keyByMilliseconds);

        env.getConfig().setGlobalJobParameters(conf);
        RocketMQSource rocketMQSource = new RocketMQSource();
        DataStream<String> rawSource = env
                .addSource(rocketMQSource).setParallelism(sourceParallelism).name("DAT_接收_Source");

        //原始Xml入Hdfs
        rawSource
                .addSink(HdfsSink.getSink(topic)).setParallelism(sinkParallelism).name("DAT_HDFS入库_Sink");

        try {
            env.execute("DAT Flow");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}