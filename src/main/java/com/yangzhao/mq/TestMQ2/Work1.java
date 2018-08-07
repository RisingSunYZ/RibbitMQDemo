package com.yangzhao.mq.TestMQ2;

import com.rabbitmq.client.*;

import java.io.IOException;

/**
 * 测试结果：
 1、消费者1和消费者2获取到的消息内容是不同的，同一个消息只能被一个消费者获取。
 2、消费者1和消费者2获取到的消息的数量是相同的，一个是奇数一个是偶数。
 其实，这样是不合理的，应该是消费者1要比消费者2获取到的消息多才对。
 Work的能者多劳模式
 需要将上面两个消费者的channel.basicQos(1);这行代码的注释打开,再次执行会发现,休眠时间短的消费者执行的任务多
 消息的确认
 在以上的代码中,已经给出了注释,如何使用自动确认和手动确认,消费者从队列中获取消息，服务端如何知道消息已经被消费呢？
 模式1：自动确认
 只要消息从队列中获取，无论消费者获取到消息后是否成功消息，都认为是消息已经成功消费。
 模式2：手动确认
 消费者从队列中获取消息后，服务器会将该消息标记为不可用状态，等待消费者的反馈，如果消费者一直没有反馈，那么该消息将一直处于不可用状态。
 如果选用自动确认,在消费者拿走消息执行过程中出现宕机时,消息可能就会丢失！！
 */
public class Work1 {

    private static final String TASK_QUEUE_NAME = "test";

    public static void main(String[] args) throws Exception {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        System.out.println("Worker1  Waiting for messages");

        // 同一时刻服务器只会发一条消息给消费者(能者多劳模式)
        channel.basicQos(1);

        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Worker1  Received '" + message + "'");
                try {
                    Thread.sleep(10); // 暂停1秒钟
                }catch (Exception e){
                    channel.abort();
                }finally {
                    // 手动返回ack包确认状态
                    channel.basicAck(envelope.getDeliveryTag(),false);
                    //channel.basicReject(); channel.basicNack(); //可以通过这两个函数拒绝消息，可以指定消息在服务器删除还是继续投递给其他消费者
                }
            }
        };
        /*
         * 监听队列，不自动返回ack包,下面手动返回
         * 如果不回复，消息不会在服务器删除
         */
        channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
    }
}
