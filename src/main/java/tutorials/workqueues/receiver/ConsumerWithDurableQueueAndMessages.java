package tutorials.workqueues.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConsumerWithDurableQueueAndMessages {
    private static final String QUEUE_NAME = "test_task_queue_1";
    // NOTE:
    // - For ensure messages is DURABLE,
    // we need to MARK BOTH the QUEUE (producer or/and consumer side) and MESSAGE (producer side) as DURABLE.

    // - To ensure that the message is not lost while being consuming (maybe consumer stop working, or throws exception)
    // uses MANUAL MESSAGE ACKNOWLEDGEMENT.
    // - Manual Acknowledgement must be sent on the SAME channel that received the delivery. Attempts to acknowledge using a
    // different channel will result in a channel-level protocol exception.

    public static void main(String[] args) throws IOException, TimeoutException {
        //1. Connection to Server (NOTE: Not in "TryWithResources" because continue listening queue)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //2. DURABLE queue declaration.
        boolean durable = true;
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null);

        //3. This method tells to RabbitMQ, not to give more than one message to a worker (consumer) AT A TIME.
        //Or, in other words, DON'T DISPATCH a new message to a worker until it HAS PROCESSED AND ACKNOWLEDGED the previous one.
        //Instead, it will dispatch (if exist) it to the next worker that is not still busy.
        channel.basicQos(1); // accept only one unack-ed message at a time.

        //4. Callback method for consuming.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");

            try {
                doWork(message);
            } finally {
                System.out.println(" [x] Done");
                //5. If this method don't call (when autoAck off) The queue fills until consumer quit (because, new messages
                // not dispatch to consumer.) and consumes more memory, if only one consumer consumes the queue.
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); //Manual acknowledgement sends.
            }
        };

        //6. "autoAck" if "true", the server should consider messages acknowledged once DELIVERED.
        boolean autoAck = false; //the server should EXPECT EXPLICIT acknowledgements.
        channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback, consumerTag -> { });

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
