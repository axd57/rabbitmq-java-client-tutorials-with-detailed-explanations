package tutorials.rpc.receiver;

import com.rabbitmq.client.*;

public class RPCServer {


    private static final String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n - 1) + fib(n - 2);
    }

    public static void main(String[] argv) throws Exception {
        //1. Connection to Server (NOTE: Not in "TryWithResources" because continue listening queue)
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //2. Queue declaration.
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

        //3. Cleaning all messages from queue.
        channel.queuePurge(RPC_QUEUE_NAME);

        //4. Only 1 non Ack message processing.
        channel.basicQos(1);

        System.out.println(" [x] Awaiting RPC requests");

        //5. Callback method for consuming.
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {

            //5.1. "correlationId" property setting with consumed message's "correlationId" property for response message, to
            //compare request and response message relation.
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";

            try {
                String message = new String(delivery.getBody(), "UTF-8");
                int n = Integer.parseInt(message);

                System.out.println(" [.] fib(" + message + ")");
                response += fib(n);
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                //5.2. routing key setting from consumed message's "replyTo" property to specify queue name (when using default exchange -> "")
                channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));

                //5.3. Manual ack setting, after process completed.
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(RPC_QUEUE_NAME, false, deliverCallback, (consumerTag -> {}));
    }
}

