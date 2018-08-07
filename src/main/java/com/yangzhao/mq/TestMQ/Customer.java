package com.yangzhao.mq.TestMQ;

import com.rabbitmq.client.*;

import java.io.IOException;

public class Customer {

    private final static String QUEUE_NAME = "rabbitMQ.test";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        // 声明队列(如果你已经明确的知道有这个队列,那么下面这句代码可以注释掉,如果不注释掉的话,也可以理解为消费者必须监听一个队列,如果没有就创建一个)
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };
        /*
         * 监听队列
         * 参数1:队列名称
         * 参数2：是否发送ack包，不发送ack消息会持续在服务端保存，直到收到ack。  可以通过channel.basicAck手动回复ack
         * 参数3：消费者
         */
        channel.basicConsume(QUEUE_NAME, true, consumer);
    }
}
