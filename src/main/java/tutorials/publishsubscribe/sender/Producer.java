package tutorials.publishsubscribe.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Producer {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1. Connection to Server
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            //2. "Fanout" exchange declaration for sending EACH message to Bound EACH queue, with ignore routing key or pattern.
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String message = "This is LOG!";

            //3. Sending message to "logs" (fanout) exchange with empty routing key (because it is being ignore from fanout exchange,
            // can be setting routing key although, but it is not be using)
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}