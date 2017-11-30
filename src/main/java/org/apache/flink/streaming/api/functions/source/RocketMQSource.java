package org.apache.flink.streaming.api.functions.source;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.config.GlobleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;

/**
 * 消费MQ的Source
 *
 * @author zhangjianxin
 * @date 2017/7/20
 */
public class RocketMQSource extends RichSourceFunction<String>
        implements ParallelSourceFunction<String> {
    private DefaultMQPushConsumer consumer;
    private static Logger logger = LoggerFactory.getLogger(RocketMQSource.class);
    private boolean isRunning = false;
    private boolean isHasData = true;
    private String charsetName;

    public RocketMQSource() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("[SOURCE-ROCKETMQ] running method open ");
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;
        GlobleConfig.getInstance(globConf.getString("confdir", null));
        if (consumer == null) {
            consumer = new DefaultMQPushConsumer(globConf.getString("cgroup", null));
            charsetName = globConf.getString("charset", "UTF-8");
            consumer.setNamesrvAddr(GlobleConfig.getMQ());
            consumer.setInstanceName(UUID.randomUUID().toString());
            consumer.setMessageModel(MessageModel.CLUSTERING);// 消费模式
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.subscribe(globConf.getString("topic", null), "*");
            consumer.setConsumeThreadMax(128);
            consumer.setConsumeThreadMin(12);
            int maxBatchSize = 1024;
            //消息数量每次读取的消息数量
            consumer.setConsumeMessageBatchMaxSize(maxBatchSize);
            logger.info("[SOURCE-ROCKETMQ] create consumer ");
        }
        logger.info("consumeBatchSize:{} pullBatchSize:{} consumeThread:{}", consumer.getConsumeMessageBatchMaxSize(), consumer.getPullBatchSize(), consumer.getConsumeThreadMax());
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        logger.info("[SOURCE-ROCKETMQ] running method run " + isRunning);
        if (!isRunning) {
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                                                                ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                    for (Message msg : list) {
                        isHasData = true;
                        try {
                            byte[] body = msg.getBody();
                            String message = new String(body, Charset.forName(charsetName));
                            logger.debug("[SOURCE-MSG] " + message);
                            logger.debug("[SOURCE-MSG-CHARSET] " + charsetName);
                            sourceContext.collect(message);
                        } catch (Exception e) {
                            e.printStackTrace();
                            // 重试
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    isHasData = false;
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            consumer.start();
            isRunning = true;
        }
        while (isRunning) {
            Thread.sleep(1000);
        }
    }


    @Override
    public void close() throws Exception {
        super.close();
        logger.info("[SOURCE-ROCKETMQ] running method close ");
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
        isRunning = false;
    }

    @Override
    public void cancel() {
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
        isRunning = false;
       /* logger.info("[SOURCE-ROCKETMQ] running method cancel ");
        while (isHasData) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        isRunning = false;*/
    }
}