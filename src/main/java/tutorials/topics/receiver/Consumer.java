package tutorials.topics.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumer {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws Exception {
        //1. Connection to Server (NOTE: Not in "TryWithResources" because continue listening queue)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //2. Topic exchange declaration.
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

        //3. Temp queue declaration.
        String queueName = channel.queueDeclare().getQueue();

        //4. Different routing patterns
        String[] routingPatterns = {"kern.*", "*.kern.*", "#.err"};

        //5. Multiple bindings.
        for (String bindingKey : routingPatterns) {
            channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
            System.out.println(" [!] \"" + queueName + "\" queue bind to \"" + EXCHANGE_NAME + "\" exchange with \"" + bindingKey + "\" binding key (routing pattern for topic exchange).");
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        //6. Callback method for consuming messages.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
}
