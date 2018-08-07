package com.yangzhao.mq.TestMQ2;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Work2 {

    private static final String TASK_QUEUE_NAME = "test";

    public static void main(String[] args) throws Exception {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        System.out.println("Worker2  Waiting for messages");

        //每次从队列获取的数量
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Worker2  Received '" + message + "'");
                try {
                    Thread.sleep(1000); // 暂停1秒钟
                }catch (Exception e){
                    channel.abort();
                }finally {
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        };
        //消息消费完成确认
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }
}
