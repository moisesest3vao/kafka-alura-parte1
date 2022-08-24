import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String email = "Thank you for you order, we are processing it";

        try(KafkaDispatcher kafkaDispatcher = new KafkaDispatcher()){
            for (int i = 0; i<10; i++){
                var key = UUID.randomUUID().toString();
                var value = key+",64545,78545543";
                kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, value);
                kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
