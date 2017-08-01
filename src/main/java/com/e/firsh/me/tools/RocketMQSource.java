package com.e.firsh.me.tools;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhangjianxin on 2017/7/20.
 */
public class RocketMQSource extends RichSourceFunction<String> implements MessageListenerConcurrently  {
    public static DefaultMQPushConsumer consumer;
    public transient LinkedBlockingQueue<String> queue;
    private static Logger LOG = LoggerFactory.getLogger(RocketMQSource.class);
    public static String consumerGroupName;
    public static String namesrvAddr;
    public static String[] topic;
    public static int ConsumeMessageBatchMaxSize;
    public static int ConsumeThreadMax;
    public static int ConsumeThreadMin;
    public static String charset;
    public RocketMQSource(String consumerGroupName,String namesrvAddr,String charset ,int ConsumeMessageBatchMaxSize,int ConsumeThreadMin,int ConsumeThreadMax,String ...topic){
        this.consumerGroupName = consumerGroupName;
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.ConsumeMessageBatchMaxSize = ConsumeMessageBatchMaxSize;
        this.ConsumeThreadMin = ConsumeThreadMin;
        this.ConsumeThreadMax = ConsumeThreadMax;
        this.charset = charset;
    }
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        final  AtomicLong[] tps = {new AtomicLong(0),new AtomicLong(0),new AtomicLong(System.currentTimeMillis())};
        byte[] body;
        String message;
        for (Message msg : list) {
            body = msg.getBody();
            message = new String(body, Charset.forName(charset));
            try {
                queue.put(message);

                tps[0].addAndGet(1);
                long now = System.currentTimeMillis();
                if(now - tps[2].get() > 2 * 1000){
                    synchronized (RocketMQSource.class){
                        if(now - tps[2].get() > 2 * 1000){
                            LOG.info("consume TPS:{} consumeTS:{}",(tps[0].get()-tps[1].get())* 1000/(now - tps[2].get()), Thread.currentThread().getId());
                            tps[1].set(tps[0].get());
                            tps[2].set(now);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;// 重试
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        queue = new LinkedBlockingQueue<>(1000);
        consumer = new DefaultMQPushConsumer(consumerGroupName);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setMessageModel(MessageModel.CLUSTERING);// 消费模式
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.registerMessageListener(this);
        for (String s : topic) {
            consumer.subscribe(s, "*");
        }
        consumer.setConsumeThreadMax(ConsumeThreadMax);
        consumer.setConsumeThreadMin(ConsumeThreadMin);
        consumer.setConsumeMessageBatchMaxSize(ConsumeMessageBatchMaxSize);//消息数量每次读取的消息数量
        System.out.println("Start");
        consumer.start();
        System.out.println("RocketMQ Started.");
        LOG.info("consumeBatchSize:{} pullBatchSize:{} consumeThread:{}",consumer.getConsumeMessageBatchMaxSize(),consumer.getPullBatchSize(),consumer.getConsumeThreadMax());
        super.open(parameters);
    }

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        String obj = "";
        long start = System.currentTimeMillis();
        int sendNum=0, lastSendNum=0;
        while (true) {
            if(queue!=null && !queue.isEmpty()){
                sendNum++;
                long now = System.currentTimeMillis();
                if(now - start > 1000){
                    LOG.info("QPS:{}" + (sendNum-lastSendNum));
                    start = now;
                    lastSendNum =sendNum;
                }
                obj = queue.take();
                sourceContext.collect(obj);
            }else {
                System.out.println("Sleep 1000ms");
                Thread.sleep(1000);
            }
        }
    }
    @Override
    public void cancel() {
        consumer.shutdown();
    }
}
