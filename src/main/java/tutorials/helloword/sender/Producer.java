package tutorials.helloword.sender;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQImpl;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String QUEUE_NAME = "test_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        //1. Connection: for TCP connection. (abstracts the socket connection)
        try (Connection connection = factory.newConnection();
            //2. Channel: message send/receive inside TCP connection.
            Channel channel = connection.createChannel()) {

            //3. Queue declaration.
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            //4. Message sending to queue.
            String message = "Hello World Test!";
            //4.1. Message sending to "AMQP default" exchange (because, 1. exchange name is -> "". 2. routing key SAME with queue name)
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
