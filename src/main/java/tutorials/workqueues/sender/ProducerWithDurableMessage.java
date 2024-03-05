package tutorials.workqueues.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerWithDurableMessage {
    private static final String QUEUE_NAME = "test_task_queue_1";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1. Connection to Server
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
            Channel channel = connection.createChannel()) {

            //2. DURABLE queue declaration.
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);

            String message = "Hello World Test!...";

            //3. With "MessageProperties.PERSISTENT_TEXT_PLAIN" parameter defines "durable message" IF QUEUE DURABLE.
            channel.basicPublish("", QUEUE_NAME,  MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        }
    }
}
