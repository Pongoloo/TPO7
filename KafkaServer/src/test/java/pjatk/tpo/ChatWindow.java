package pjatk.tpo;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChatWindow extends JFrame {
    private JPanel mainPanel;
    private JButton sendButton;
    private JTextArea messageField;
    private JButton loginButton;
    private JTextField textField1;
    private JComboBox availableChats;
    private JTextArea chatView;
    private JButton chatSwitchButton;
    private final MessageConsumer messageConsumer;
    private final MessageConsumer metaDataConsumer;

    public ChatWindow(String topic, String id) {
        messageConsumer = new MessageConsumer(topic, id);
        metaDataConsumer = new MessageConsumer("metadata", id);
        this.setDefaultCloseOperation(EXIT_ON_CLOSE);
        this.setPreferredSize(new Dimension(600, 800));
        this.add(mainPanel);
        this.setVisible(true);
        this.setTitle("WOop");
        this.pack();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> {

        });
        executorService.submit(() -> {
            while (true) {
                metaDataConsumer.kafkaConsumer.poll(Duration.of(1, ChronoUnit.SECONDS)).forEach(m -> {
                    System.out.println(m.value());
                    availableChats.addItem(m.value());
                });
            }
        });
        sendButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                MessageProducer.send(new ProducerRecord<>("chat", id + " " + messageField.getText().strip()));
            }
        });

        chatSwitchButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                messageConsumer.kafkaConsumer.subscribe(Collections.singleton(availableChats.getModel().getSelectedItem().toString()));
            }
        });
    }
    private void processMessage(String message){
        if(message.startsWith("SYSTEM.INFO Created chat")){
            int chatNameBeginIndex = message.indexOf(":");

        }
    }
}

