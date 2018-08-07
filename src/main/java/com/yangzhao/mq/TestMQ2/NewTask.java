package com.yangzhao.mq.TestMQ2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * work模式
 */
public class NewTask {

    public final static String QUEUE_NAME="test";

    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        for(int i=0;i<10;i++){
            String message = "Hello World!"+i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");

            Thread.sleep(i*100);
        }

        //关闭通道和连接
        channel.close();
        connection.close();
    }

}
