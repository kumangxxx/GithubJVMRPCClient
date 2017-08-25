import java.util.concurrent.*;

public class Demo {

    public static void main(String[] args) {

        GITSRabbitRPCClient client = null;

        try {
            client = new GITSRabbitRPCClient("amqps://gitsmicros:UNIKOM@portal-ssl1155-0.bmix-dal-yp-151044e0-030b-4406-8efc-84656da093b9.nancys-us-ibm-com.composedb.com:19324/bmix-dal-yp-151044e0-030b-4406-8efc-84656da093b9");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (client != null) {
            client.callExchange("java_exc", "java.test", "Wasap boy", 2, new GITSRabbitRPCClient.RPCCallback() {
                @Override
                public void onSuccess(String message) {
                    System.out.println("Response received : " + message);
                }

                @Override
                public void onError(Exception e) {
                    e.printStackTrace();
                }
            });
        } else {
            System.out.println("Client is null");
        }
    }

}
