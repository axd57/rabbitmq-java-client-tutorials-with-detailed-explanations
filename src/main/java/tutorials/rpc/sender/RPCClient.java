package tutorials.rpc.sender;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;

public class RPCClient implements AutoCloseable {

    private Connection connection;
    private Channel channel;
    private String requestQueueName = "rpc_queue";

    //1. Connection to Server
    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    public static void main(String[] argv) {
        try (RPCClient fibonacciRpc = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String val = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + val + ")");
                String response = fibonacciRpc.call(val);
                System.out.println(" [.] Got '" + response + "'");
            }
        } catch (IOException | TimeoutException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    //2. RPC server request
    public String call(String message) throws IOException, InterruptedException, ExecutionException {
        //2.1. Random value for "correlationId" property for MATCH REQUEST AND RESPONSE messages.
        final String corrId = UUID.randomUUID().toString();

        //2.2. Temp queue declaration for server's reply responses.
        String replyQueueName = channel.queueDeclare().getQueue();
        System.out.println(" [!] reply(Temp) queue name: " + replyQueueName);

        //2.3. Message property declaration.
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(corrId) // For matching requests and responses.
                .replyTo(replyQueueName) // For specify RPC serve's response message publish queue name.
                .build();

        //2.4. Message publish to RPC server queue (with using AMQP Default exchange).
        channel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));


        //2.5. to suspend the "Main" thread before the response arrives.
        //When calls "complete" method of "CompletableFuture", "CompletableFuture" ends and "Main" thread continues.
        //Then callback method exit to listening queue.
        final CompletableFuture<String> response = new CompletableFuture<>();

        //2.6. Reply queue listening for RPC server's right request's right response (with compare "CorrelationId" property)
        String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.complete(new String(delivery.getBody(), "UTF-8"));
            }
        }, consumerTag -> {});

        String result = response.get();

        //2.7. Consumer stop's to consuming temp queue (unsubscribe). Then temp queue will remove automatically by server.
        channel.basicCancel(ctag);

        return result;
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }
}
