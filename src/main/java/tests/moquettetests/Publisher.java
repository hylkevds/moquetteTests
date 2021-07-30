package tests.moquettetests;

import io.moquette.broker.Server;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final MqttClient client;
    private final Server broker;
    private final String topic;
    /**
     * Flag to indicate we think it is working, so we don't keep complaining
     * about the same worker.
     */
    private boolean working = true;
    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong batchCount = new AtomicLong();
    private Thread current;

    public Publisher(String serverUrl, String clientId, String topic) throws MqttException {
        this.clientId = clientId;
        this.topic = topic;
        this.client = new MqttClient(serverUrl, clientId, new MemoryPersistence());
        this.broker = null;
    }

    public Publisher(Server broker, String clientId, String topic) throws MqttException {
        this.clientId = clientId;
        this.topic = topic;
        this.broker = broker;
        this.client = null;
    }

    @Override
    public void run() {
        current = Thread.currentThread();
        LOGGER.info("Publishing on: {}", topic);
        stop = false;
        while (!stop) {
            String message = "Last: " + lastMessage + MoquetteTest.MESSAGE;
            if (client != null) {
                publishClient(message);
            }
            if (broker != null) {
                publishDirect(message);
            }
            sleep(MoquetteTest.PUBLISHER_SLEEP_MILLIS);
        }
        LOGGER.info("Publisher {} exiting: {}", clientId, topic);
        try {
            if (client != null && client.isConnected()) {
                client.disconnect(1000L);
            }
        } catch (MqttException ex) {
            LOGGER.error("Failed to close pubish client", ex);
        }
    }

    private void publishDirect(String message) {
        for (int i = 0; i < MoquetteTest.PUBLISHER_BATCH_COUNT && !stop; i++) {
            final ByteBuf payload = ByteBufUtil.writeUtf8(UnpooledByteBufAllocator.DEFAULT, message);
            MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.valueOf(MoquetteTest.QOS_PUBLISH), false, 0);
            MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(topic, 0);
            MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);
            broker.internalPublish(mqttPublishMessage, clientId);
            sentCount.incrementAndGet();
            lastMessage = System.currentTimeMillis();
        }
        batchCount.incrementAndGet();
    }

    private void publishClient(String message) throws IllegalArgumentException {
        try {
            if (!client.isConnected()) {
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                connOpts.setKeepAliveInterval(30);
                connOpts.setConnectionTimeout(30);
                connOpts.setMaxInflight(MoquetteTest.MAX_IN_FLIGHT);
                client.connect(connOpts);
            }
            for (int i = 0; i < MoquetteTest.PUBLISHER_BATCH_COUNT && !stop; i++) {
                if (client.isConnected()) {
                    client.publish(topic, message.getBytes(MoquetteTest.UTF8), MoquetteTest.QOS_PUBLISH, false);
                    sentCount.incrementAndGet();
                } else {
                    LOGGER.info("Publisher {} lost connection: {}", clientId, topic);
                }
                lastMessage = System.currentTimeMillis();
            }
            batchCount.incrementAndGet();
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
