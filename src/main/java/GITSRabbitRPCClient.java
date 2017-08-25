import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.*;



public class GITSRabbitRPCClient {

    private Connection globalConnection;
    private Channel globalChannel;

    interface RPCCallback {
        public void onSuccess(String message);
        public void onError(Exception e);
    }

    public GITSRabbitRPCClient(String uri) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(uri);
        globalConnection = factory.newConnection();
        globalChannel = globalConnection.createChannel();
    }



    public void callExchange(
            String exchangeName,
            String routeKey,
            String body,
            int timeout,
            RPCCallback callback
    ) {
        ExecutorService service = Executors.newSingleThreadExecutor();
        ExchangeCall ecall = new ExchangeCall(exchangeName, routeKey, body, timeout, callback);
        Future<Integer> f = service.submit(ecall);

        try {
            System.out.println("trying");
            f.get(timeout, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            e.printStackTrace();
            callback.onError(new Exception("timeout"));
            ecall.setTimedout(true);
        } catch (Exception e) {
            e.printStackTrace();
            callback.onError(e);
            ecall.setTimedout(true);
        }

        service.shutdown();
    }

    private void rpcCall(String exchangeName, String routeKey, String body, RPCCallback callback) {

        String correlationId = UUID.randomUUID().toString();

        try {
            String queueName = globalChannel.queueDeclare().getQueue();
            System.out.println("waiting response in queue : " + queueName);
            globalChannel.basicConsume(queueName, true, new DefaultConsumer(globalChannel) {
                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties, byte[] body
                ) throws IOException {
                    String message = new String(body);
                    System.out.println("incoming message with body : " + message);
                    if (properties.getCorrelationId().equals(correlationId)) {
                        callback.onSuccess(message);
                    }
                }
            });

            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(queueName)
                    .build();

            globalChannel.basicPublish(exchangeName, routeKey, properties, body.getBytes("UTF-8"));

        } catch (Exception e) {
            callback.onError(e);
        }

    }

    class ExchangeCall implements Callable<Integer> {

        RPCCallback callback;
        int timeout;
        String exchangeName, routeKey, body;
        boolean timedout = false;

        public void setTimedout(boolean timedout) {
            this.timedout = timedout;
        }

        public void setCallback(RPCCallback callback) {
            this.callback = callback;
        }

        public ExchangeCall(String exchangeName, String routeKey, String body, int timeout, RPCCallback callback) {
            this.callback = callback;
            this.timeout = timeout;
            this.exchangeName = exchangeName;
            this.routeKey = routeKey;
            this.body = body;
        }

        @Override
        public Integer call() throws Exception {
            try {
                rpcCall(exchangeName, routeKey, body, new RPCCallback() {
                    @Override
                    public void onSuccess(String message) {
                        if (timedout == false)
                            callback.onSuccess(message);
                    }

                    @Override
                    public void onError(Exception e) {
                        if (timedout == false)
                            callback.onError(e);
                    }
                });
            } catch(Exception e) {
                if (timedout == false)
                    callback.onError(e);
            }

            System.out.println("waiting");
            Thread.sleep((timeout + 1) * 1000);
            System.out.println("Returning");
            return 0;
        }
    }

}
