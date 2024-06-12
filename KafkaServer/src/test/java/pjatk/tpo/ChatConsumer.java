package pjatk.tpo;

import javax.swing.*;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class ChatConsumer implements Runnable{
    private final MessageConsumer messageConsumer;
    Chat

    public ChatConsumer(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public void run() {
        while (true) {
            messageConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                System.out.println(m.value());
                chatView.append(m.value() + '\n');
            });
        }
    }
}
