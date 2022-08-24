import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

public class FraudDetectorService {

    public static void main(String[] args) {
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        try(KafkaService<Order> kafkaService = new KafkaService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of()
        )){
            kafkaService.run();

        }
    }

    private void parse(ConsumerRecord<String, Order> record){
        System.out.println("====================");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(record.timestamp());
        try{
            Thread.sleep(2000);
        }catch (Exception e){
            //ignore
            e.printStackTrace();
        }
        System.out.println("Order has been processed");
    }
}
