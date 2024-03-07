package tutorials.publishsubscribe.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        //1. Connection to Server (NOTE: Not in "TryWithResources" because continue listening queue)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //2. exchange declaration.
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

        //3. Temp queue declaration, its features:
        //- non-durable: the queue not survive a server restart, exclusive: can only be used (consumed, purged, deleted, etc.) by its declaring connection.
        //- auto-delete: server will delete it when no longer in use (when last consumer unsubscribed).
        //- and randomly named.
        String queueName = channel.queueDeclare().getQueue();
        System.out.println(" [!] Temp queue name is : " + queueName);

        //3. Queue and exchange binding with NOT specifying (or any) routing key because exchange type is "fanout".
        channel.queueBind(queueName, EXCHANGE_NAME, "not_using_routing_key"); //Same with empty rounding key parameter.

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //4. Callback method for consuming.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
