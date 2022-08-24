import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(KafkaDispatcher<Order> kafkaDispatcher = new KafkaDispatcher<>()){
            try(KafkaDispatcher<Email> emailKafkaDispatcher = new KafkaDispatcher<>()){
                for (int i = 0; i<10; i++){
                    var orderId = UUID.randomUUID().toString();
                    var userId = UUID.randomUUID().toString();
                    var amount = Math.random()*5000+1;

                    Order order = new Order(userId,orderId, new BigDecimal(amount));
                    kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                    String body = "Thank you for you order, we are processing it";
                    String subject = "Order Processing";
                    Email email = new Email(subject, body);
                    emailKafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }
}
