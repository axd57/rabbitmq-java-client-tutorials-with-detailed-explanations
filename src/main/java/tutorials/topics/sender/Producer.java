package tutorials.topics.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        //1. Connection to Server
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            //2. "Topic" exchange declaration for sending message to MATCHED ROUTING PATTERN binding queue(s).
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            String routingPattern = "root.kern.err";
            String message = "From Root kernel";

            channel.basicPublish(EXCHANGE_NAME, routingPattern, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}