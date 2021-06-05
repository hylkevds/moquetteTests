/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tests.moquettetests;

import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static tests.moquettetests.MoquetteTest.CLIENT_CLEAN_SESSION;

/**
 *
 * @author hylke
 */
class Listener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Listener.class.getName());

    private final String clientId;
    private final String[] topics;
    private final MqttClient client;
    private final AtomicLong recvCount = new AtomicLong();
    private boolean stop = false;
    private Thread current;

    public Listener(String serverUrl, String clientId, String[] topics) throws MqttException {
        this.clientId = clientId;
        this.topics = topics;
        this.client = new MqttClient(serverUrl, clientId, new MemoryPersistence());
    }

    public void stop() {
        stop = true;
        try {
            current.interrupt();
        } catch (SecurityException ex) {
            LOGGER.info("Failed to interrupt.");
        }
        current = null;
    }

    public long getRecvCount() {
        return recvCount.get();
    }

    @Override
    public void run() {
        current = Thread.currentThread();
        LOGGER.info("Listening on {} topics", topics.length);
        while (!stop) {
            try {
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(CLIENT_CLEAN_SESSION);
                connOpts.setKeepAliveInterval(30);
                connOpts.setConnectionTimeout(30);
                client.connect(connOpts);
                for (String topic : topics) {
                    client.subscribe(topic, MoquetteTest.QOS_LISTEN, (String msgTopic, MqttMessage message) -> {
                        recvCount.incrementAndGet();
                        String msg = message.toString();
                        if (!msg.startsWith("Last") || !msg.endsWith(MoquetteTest.MESSAGE)) {
                            LOGGER.error("Incorrect message: {}", msg);
                        }
                        LOGGER.trace("{} Received message on {}", clientId, msgTopic);
                    });
                }
                LOGGER.info("Subscribed to {} topics.", topics.length);
                if (!stop) {
                    sleep(MoquetteTest.CLIENT_LIVE_MILLIS);
                }
                client.unsubscribe(topics);
                client.disconnect();
                if (!stop) {
                    sleep(MoquetteTest.CLIENT_DOWN_MILLIS);
                }
            } catch (MqttException ex) {
                LOGGER.error("Client Failed", ex);
            }
        }
    }

    private void sleep(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
        }
    }

}
