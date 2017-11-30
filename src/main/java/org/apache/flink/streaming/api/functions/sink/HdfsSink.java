package org.apache.flink.streaming.api.functions.sink;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

/**
 * Created by 张建新 on 2017/10/31.
 *
 * @author 张建新
 */
public class HdfsSink {

    /**
     * 通用入HDFS-Map
     * @param topic topic名
     * @return SaveHdfsSink
     */
    public static SinkFunction<String> getSink(String topic) {
        Configuration configuration = new Configuration();
        //TODO 添加Hadoop配置内容
        configuration.setString("dfs.namenode.name.dir", "file:///home/hadmin/data/hadoop/hdfs/name");
        configuration.setString("dfs.nameservices", "ns");
        configuration.setString("dfs.ha.namenodes.ns", "nn1,nn2");
        configuration.setString("dfs.namenode.rpc-address.ns.nn1", "10.11.0.193:9000");
        configuration.setString("dfs.namenode.rpc-address.ns.nn2", "10.11.0.194:9000");
        configuration.setString("dfs.namenode.shared.edits.dir", "qjournal://10.11.0.193:8485;10.11.0.194;10.11.0.195:8485/ns");
        configuration.setString("hadoop.tmp.dir", "/home/hadmin/data/hadoop/tmp");
        configuration.setString("fs.defaultFS", "hdfs://ns");
        configuration.setString("dfs.journalnode.edits.dir", "/home/hadmin/data/hadoop/journal");
        configuration.setString("ha.zookeeper.quorum", "10.11.0.193:2181,10.11.0.194:2181,10.11.0.195:2181");
        configuration.setString("mapreduce.input.fileinputformat.split.minsize", "10");

        TODBucketingSink<String> sink = new TODBucketingSink<>("/xml/" + topic + "/");
        sink.setBucketer(new DateTimeBucketer<>("yyyy/MM/dd/HH"));
        sink.setWriter(new StringWriter<>());
        sink.setPendingPrefix("source");
        sink.setPendingSuffix(".txt");
        sink.setFSConfig(configuration);
        //设置Flink Bucketer 的刷新时间
        sink.setAsyncTimeout(60000L);
        return sink;
    }

}
