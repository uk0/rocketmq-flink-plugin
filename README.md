

# rocketmq-flink-source-plugin

RocketMQSource

该工具为第一个版本作为雏形,本人会陆续修改,目前还在寻找更好的解决方案
 
# 快速开始
 * 在Flink中调用即可，参数通过构造器传入.
```java
   /**
    DataStream<String> rockMQStream = sourceStream
                   .map(new NuiFlier());
           rockMQStream.addSink(new RocketMQSource(args...));
   */
```

# 未完成(以下会同步完成Source)
 *  `RocketMQ Sink`,`Mongodb Sink`,` Hdfs Sink`,`CrateDB Sink`,` Hbase Sink `
 * 目前只是一个`Demo` 希望大家多多支持,详情关注博客.
 
 * ![https://firsh.me]