package com.yangzhao.mq.TestMQ3;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * 订阅模式
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";
    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        /*
         * 声明exchange(交换机)
         * 参数1：交换机名称
         * 参数2：交换机类型
         * 参数3：交换机持久性，如果为true则服务器重启时不会丢失
         * 参数4：交换机在不被使用时是否删除
         * 参数5：交换机的其他属性
         */
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        // 与前面不同, 生产者将消息发送给exchange, 而非队列. 若发消息时还没消费者绑定queue与该exchange, 消息将丢失

        for(int i=0;i<5;i++){
            String message = "Hello"+i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
            Thread.sleep(1000);
        }

        channel.close();
        connection.close();
    }
}
