import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        FraudDetectorService fraudDetectorService = new FraudDetectorService();
        try(KafkaService kafkaService = new KafkaService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse
        )){
            kafkaService.run();

        }
    }

    private void parse(ConsumerRecord<String, String> record){
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
