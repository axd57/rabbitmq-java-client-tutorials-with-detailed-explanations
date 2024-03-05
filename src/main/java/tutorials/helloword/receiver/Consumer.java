package tutorials.helloword.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String QUEUE_NAME = "test_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1. Connection to Server (NOTE: Not in "TryWithResources" because continue listening queue)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //2. Same queue declaration on consumer, like producer. For MAKE SURE QUEUE EXIST WHEN STARTING LISTENING (consuming).
        //if queue already exist, this declaration NOT using (NOTE: if queue DECLARATION exist both producer and consumer side
        //MUST BE defines with EXACTLY SAME configuration, otherwise RabbitMQ throws "ShutdownSignalException"), both producer or consumer sides.
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        //3. Callback interface to be notified when a message is delivered.
        //That will buffer the messages until we're ready to use them.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "' (Consumer tag: " + consumerTag + ")");
        };

        //4. Using exchange "AMQP default". For this reason not explicitly any exchange binding defines, binding makes
        //automatically from server.
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }
}
