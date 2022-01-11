package tests.moquettetests;

import com.hivemq.client.mqtt.MqttClientBuilder;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import io.moquette.broker.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static tests.moquettetests.MoquetteTest.PUBLISHER_BATCH_COUNT;
import static tests.moquettetests.MoquetteTest.PUBLISHER_BATCH_COUNT_INCREMENT;

/**
 *
 * @author hylke
 */
class Publisher implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Publisher.class.getName());

    /**
     * The time the last message was sent. No need for synchronisation,
     * precision is not important.
     */
    private long lastMessage = 0;
    private boolean stop = false;
    private final String clientId;
    private final MqttClient clientPaho;
    private final Mqtt3BlockingClient clientHiveMq;
    private final Server broker;
    private final String topic;
    private long batchCountCurrent = PUBLISHER_BATCH_COUNT_INCREMENT;
    /**
     * Flag to indicate we think it is working, so we don't keep complaining
     * about the same worker.
     */
    private boolean working = true;
    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong batchCount = new AtomicLong();
    private Thread current;

    public Publisher(String serverUrl, String clientId, String topic) throws MqttException, URISyntaxException {
        this.clientId = clientId;
        this.topic = topic;
        if (MoquetteTest.USE_HIVEMQ_CLIENT) {
            URI serverUri = new URI(serverUrl);
            this.clientHiveMq = Mqtt3Client.builder()
                    .identifier(clientId)
                    .serverHost(serverUri.getHost())
                    .serverPort(serverUri.getPort())
                    .addDisconnectedListener((context) -> {
                        LOGGER.info("connectionLost");
                    })
                    .buildBlocking();
            this.clientPaho = null;
        } else {
            this.clientHiveMq = null;
            this.clientPaho = new MqttClient(serverUrl, clientId, new MemoryPersistence());
        }
        this.broker = null;
    }

    public Publisher(Server broker, String clientId, String topic) throws MqttException {
        this.clientId = clientId;
        this.topic = topic;
        this.broker = broker;
        this.clientPaho = null;
        this.clientHiveMq = null;
    }

    @Override
    public void run() {
        current = Thread.currentThread();
        LOGGER.info("Publishing on: {}", topic);
        stop = false;
        while (!stop) {
            String message = "Last: " + lastMessage + MoquetteTest.MESSAGE;
            if (clientHiveMq != null) {
                publishHiveMq(message);
            }
            if (clientPaho != null) {
                publishPaho(message);
            }
            if (broker != null) {
                publishDirect(message);
            }
            sleep(MoquetteTest.PUBLISHER_SLEEP_MILLIS);
        }
        LOGGER.info("Publisher {} exiting: {}", clientId, topic);
        try {
            if (clientPaho != null && clientPaho.isConnected()) {
                clientPaho.disconnect(1000L);
            }
        } catch (MqttException ex) {
            LOGGER.error("Failed to close pubish client", ex);
        }
    }

    private void publishDirect(String message) {
        for (int i = 0; i < batchCountCurrent && !stop; i++) {
            final ByteBuf payload = ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, message);
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(MoquetteTest.QOS_PUBLISH), false, 0);
            MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, 0);
            MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
            broker.internalPublish(mqttPublishMessage, clientId);
            sentCount.incrementAndGet();
            lastMessage = System.currentTimeMillis();
        }
        batchCount.incrementAndGet();
        if (batchCountCurrent < PUBLISHER_BATCH_COUNT) {
            batchCountCurrent += PUBLISHER_BATCH_COUNT_INCREMENT;
            batchCountCurrent = Math.min(batchCountCurrent, PUBLISHER_BATCH_COUNT);
        }
    }

    private void publishHiveMq(String message) throws IllegalArgumentException {
        if (!clientHiveMq.getState().isConnected()) {
            clientHiveMq.connectWith()
                    .cleanSession(true)
                    .send();
        }
        for (int i = 0; i < batchCountCurrent && !stop; i++) {
            if (clientHiveMq.getState().isConnected()) {
                clientHiveMq.publishWith()
                        .topic(topic)
                        .payload(message.getBytes(MoquetteTest.UTF8))
                        .qos(MqttQos.fromCode(MoquetteTest.QOS_PUBLISH))
                        .send();
                sentCount.incrementAndGet();
            } else {
                LOGGER.info("Publisher {} lost connection: {}", clientId, topic);
            }
            lastMessage = System.currentTimeMillis();
        }
        batchCount.incrementAndGet();
        if (batchCountCurrent < PUBLISHER_BATCH_COUNT) {
            batchCountCurrent += PUBLISHER_BATCH_COUNT_INCREMENT;
            batchCountCurrent = Math.min(batchCountCurrent, PUBLISHER_BATCH_COUNT);
        }
    }

    private void publishPaho(String message) throws IllegalArgumentException {
        try {
            if (!clientPaho.isConnected()) {
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                connOpts.setKeepAliveInterval(30);
                connOpts.setConnectionTimeout(30);
                connOpts.setMaxInflight(MoquetteTest.MAX_IN_FLIGHT_PUBLISHERS);
                clientPaho.connect(connOpts);
            }
            for (int i = 0; i < batchCountCurrent && !stop; i++) {
                if (clientPaho.isConnected()) {
                    clientPaho.publish(topic, message.getBytes(MoquetteTest.UTF8), MoquetteTest.QOS_PUBLISH, false);
                    sentCount.incrementAndGet();
                } else {
                    LOGGER.info("Publisher {} lost connection: {}", clientId, topic);
                }
                lastMessage = System.currentTimeMillis();
            }
            batchCount.incrementAndGet();
            if (batchCountCurrent < PUBLISHER_BATCH_COUNT) {
                batchCountCurrent += PUBLISHER_BATCH_COUNT_INCREMENT;
                batchCountCurrent = Math.min(batchCountCurrent, PUBLISHER_BATCH_COUNT);
            }
        } catch (MqttException ex) {
            LOGGER.error("Failed to send message.", ex);
        }
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

    public long getLastMessage() {
        return lastMessage;
    }

    public long getSentCount() {
        return sentCount.get();
    }

    public long getBatchCount() {
        return batchCount.get();
    }

    public boolean isStopped() {
        return stop;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isWorking() {
        return working;
    }

    public void setWorking(boolean working) {
        this.working = working;
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
