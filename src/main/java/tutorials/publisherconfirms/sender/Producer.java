package tutorials.publisherconfirms.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public class Producer {
    //  Publisher confirms are a RabbitMQ extension to implement reliable publishing (to make sure published messages have safely reached the broker).
    //  When publisher confirms are ENABLED ON A CHANNEL, messages the CLIENT PUBLISHES are CONFIRMED ASYNCHRONOUSLY by THE BROKER,
    //  meaning they have been TAKEN CARE of on the SERVER SIDE.

    static final int MESSAGE_COUNT = 1_000;


    // Connecting to server.
    static Connection createConnection() throws Exception {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setHost("localhost");
        cf.setUsername("guest");
        cf.setPassword("guest");
        return cf.newConnection();
    }

    public static void main(String[] args) throws Exception {
        //Typical techniques are:

        // publishing messages individually, waiting for the confirmation synchronously: simple, but very limited throughput.
        // publishing messages in batch, waiting for the confirmation synchronously for a batch: simple, reasonable throughput,
        // but hard to reason about when something goes wrong.
        //asynchronous handling: best performance and use of resources, good control in case of error, but can be involved to
        // implement correctly.



        // This technique is very straightforward but also has a major drawback: it SIGNIFICANTLY SLOWS DOWN PUBLISHING,
        // as the confirmation of a message BLOCKS THE PUBLISHING OF ALL SUBSEQUENT MESSAGES. This approach is not going
        // to deliver throughput of more than a few hundreds of published messages per second. Nevertheless, this can be
        // good enough for some applications.

        publishMessagesIndividually();


        // Waiting for a BATCH of messages to be confirmed IMPROVES THROUGHPUT DRASTICALLY OVER WAITING FOR A CONFIRM FOR
        // INDIVIDUAL MESSAGE (up to 20-30 times with a remote RabbitMQ node). one drawback is that WE DO NOT KNOW EXACTLY
        // WHAT WENT WRONG IN CASE OF FAILURE, so we may have to keep a whole batch in memory to log something meaningful
        // or to re-publish the messages. And this solution is still synchronous, so it blocks the publishing of messages.

        publishMessagesInBatch();


        // The broker confirms published messages asynchronously, one just needs to register a callback on the client to
        // be notified of these confirms

        handlePublishConfirmsAsynchronously();
    }

    // Strategy #1
    static void publishMessagesIndividually() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            // Random named queue declaration.
            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            // Publisher confirms are a RabbitMQ extension to the AMQP 0.9.1 protocol.
            // Not enabled by default. To enable the method calls (confirmSelect()) only ONE time for PER CHANEL.

            // When sending message with Publisher confirms (using waitForConfirmsOrDie()), if this method not calls
            // waitForConfirmsOrDie() method throws "IllegalStateException" exception.
            ch.confirmSelect();

            // Timeout for server's confirmation status (ack'd or nack'd)
            long timeout = 5_000;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                System.out.println(" [!] Message published to default exchange.");
                System.out.println(" [!] Server confirmation waiting for message. (timeout " + timeout + " ms)");
                try {
                    // The client actually receives (FRom rabbitMQ server) confirms asynchronously.
                    // Thinks of "waitForConfirmsOrDie" AS A SYNCHRONOUS HELPER which relies on ASYNCHRONOUS NOTIFICATIONS UNDER THE HOOD.
                    // Waits until ALL MESSAGES published since the LAST CALL have been either "ack'd" or "nack'd" BY THE BROKER.
                    ch.waitForConfirmsOrDie(timeout);

                // If the timeout expires a TimeoutException is thrown.
                } catch (TimeoutException e) {
                    System.out.println(" [!] timeout (" + timeout + " ms) expired.");

                // If ANY of the MESSAGES were "nack'd", throw an IOException.
                } catch (IOException e) {
                    System.out.println(" [!] Message nack'd from server.");
                }

                System.out.println(" [!] Server confirmed the message.");

            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    // Strategy #2
    static void publishMessagesInBatch() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            // Random named queue.
            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            //publisher confirmation enables.
            ch.confirmSelect();

            // bath size for divide server confirmation checks (REDUCE SYNCHRONOUS WAITING and INCREASE MESSAGE SENDING THROUGHPUT)
            int batchSize = 100;
            int outstandingMessageCount = 0;

            // Timeout for server's confirmation status (ack'd or nack'd)
            long timeout = 5_000;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);
                ch.basicPublish("", queue, null, body.getBytes());
                outstandingMessageCount++;

                if (outstandingMessageCount == batchSize) {
                    System.out.println(" [!] " + batchSize + " Messages published to default exchange.");

                    System.out.println(" [!] Server confirmation status checking for " + outstandingMessageCount + " messages. (timeout " + timeout + " ms)");
                    ch.waitForConfirmsOrDie(timeout);
                    // may be IOException (throws if any message in batch, server returns for "nack'd") or
                    // TimeoutException (throws if server can not return all message's (in batch) confirmation results in specified timeout time.)
                    // ...

                    System.out.println(" [!] Server confirmed " + outstandingMessageCount + " messages.");

                    outstandingMessageCount = 0;
                }
            }

            if (outstandingMessageCount > 0) {
                System.out.println(" [!] Server confirmation status checking for " + outstandingMessageCount + " messages.");
                ch.waitForConfirmsOrDie(timeout);
                // may be IOException (throws if any message in batch, server returns for "nack'd") or
                // TimeoutException (throws if server can not return all message's (in batch) confirmation results in specified timeout time.)
                // ...

                System.out.println(" [!] Server confirmed " + outstandingMessageCount + " messages. (timeout " + timeout + " ms)");
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    // Strategy #3
    static void handlePublishConfirmsAsynchronously() throws Exception {
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            // Random named queue declaration.
            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            //  //publisher confirmation enables.
            ch.confirmSelect();

            // Using for concurrent support and CORRELATE the PUBLISHING SEQUENCE NUMBER WITH A MESSAGE.
            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();



            // sequenceNumber: Server's (RabbitMQ node) confirmation returned MESSAGE's or LAST MESSAGE's (between  multiple messages,
            // if "multiple" parameters is "true") sequence number.

            // multiple: if false, for only one message confirmation returned (confirmed or nack-ed). If true, all messages
            // with a lower or equal sequence number are confirmed/nack-ed.


            //ACK
            // Triggers when server's confirm message.
            ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
                if (multiple) {
                    System.out.println(" [!] Sequence number's until " + sequenceNumber + " (inclusive) messages confirmed (ack) from server.");
                    ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(sequenceNumber, true);
                    confirmed.clear();
                    System.out.println(" [!] Confirmed messages cleared from \"outstandingConfirms\" map.");

                } else {
                    System.out.println(" [!] Sequence number " + sequenceNumber + " message confirmed (ack) from server.");
                    outstandingConfirms.remove(sequenceNumber);
                    System.out.println(" [!] Confirmed message cleared from \"outstandingConfirms\" map.");
                }
            };

            //NACK
            // Trigger when messages lost by the broker (confirmation result nack-ed).
            // Note: the lost messages (nack-ed) COULD STILL have been DELIVERED TO CONSUMERS, but the BROKER CANNOT GUARANTEE this.
            ConfirmCallback nackMessages = (sequenceNumber, multiple) -> {
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format("Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n", body, sequenceNumber, multiple
                );

                //Whether messages are confirmed or nack-ed, their corresponding entries in the map must be removed.
                cleanOutstandingConfirms.handle(sequenceNumber, multiple);
                System.out.println(" [!] Nack-ed message cleared from \"outstandingConfirms\" map.");
            };

            // For listening server's message confirmations asynchronously.
            // First parameter callback triggers when message(s) ack-ed from server.
            // Second parameter callback triggers when message(s) nack-ed from server.
            ch.addConfirmListener(cleanOutstandingConfirms, nackMessages);

            long timeout = 60;

            long start = System.nanoTime();
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String body = String.valueOf(i);

                // In confirm mode published message sequence no. (example: 1,2,3,...)
                long sequenceNumber = ch.getNextPublishSeqNo();

                // For tracking, the publishing sequence number before publishing a message.
                outstandingConfirms.put(sequenceNumber, body);
                System.out.println(" [!] Message added to \"outstandingConfirms\" map, with " + sequenceNumber + " sequence number.");

                ch.basicPublish("", queue, null, body.getBytes());
                System.out.println(" [!] Message sent to default exchange.");
            }

            if (!waitUntil(Duration.ofSeconds(timeout), () -> outstandingConfirms.isEmpty())) {
                throw new IllegalStateException("All messages could not be confirmed in " + timeout + " seconds");
            }

            long end = System.nanoTime();
            System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }
    }

    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited += 100;
        }
        return condition.getAsBoolean();
    }
}
