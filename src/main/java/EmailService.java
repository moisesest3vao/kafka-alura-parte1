import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {
    public static void main(String[] args) {
        EmailService emailService = new EmailService();
        try(KafkaService kafkaService = new KafkaService(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse
        )) {
            kafkaService.run();
        }
    }
    //lógica para enviar email
    private void parse(ConsumerRecord<String, String> record){
        System.out.println("====================");
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(record.timestamp());
        try{
            Thread.sleep(1000);
        }catch (Exception e){
            //ignore
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }


}
