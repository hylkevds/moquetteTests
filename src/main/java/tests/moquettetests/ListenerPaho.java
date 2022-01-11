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
class ListenerPaho implements Listener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListenerPaho.class.getName());

    private final String clientId;
    private final String[] topics;
    private final MqttClient client;
    private final AtomicLong recvCount = new AtomicLong();
    private final AtomicLong recvUnwantedCount = new AtomicLong();
    private boolean stop = false;
    private Thread current;
    private boolean lastWasClean = false;
    private long nextSleep = MoquetteTest.CLIENT_LIVE_MILLIS_INITIAL;

    public ListenerPaho(String serverUrl, String clientId, String[] topics) throws MqttException {
        this.clientId = clientId;
        this.topics = topics;
        this.client = new MqttClient(serverUrl, clientId, new MemoryPersistence());
    }

    @Override
    public void stop() {
        stop = true;
        try {
            current.interrupt();
        } catch (SecurityException ex) {
            LOGGER.info("Failed to interrupt.");
        }
        current = null;
    }

    @Override
    public long getRecvCount() {
        return recvCount.get();
    }

    @Override
    public long getUnwantedCount() {
        return recvUnwantedCount.get();
    }

    private boolean shouldCleanSession() {
        switch (CLIENT_CLEAN_SESSION) {
            case YES:
                return true;

            case NO:
                return false;

            case ALTERNATE:
            default:
                lastWasClean = !lastWasClean;
                return lastWasClean;
        }
    }

    @Override
    public void run() {
        current = Thread.currentThread();
        LOGGER.info("Listening on {} topics", topics.length);
        while (!stop) {
            final boolean shouldCleanSession = shouldCleanSession();
            try {
                connectToServer(shouldCleanSession);
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
                LOGGER.debug("Subscribed to {} topics with cleanSession={}.", topics.length, shouldCleanSession);
                if (!stop) {
                    sleep(nextSleep);
                    nextSleep = MoquetteTest.CLIENT_LIVE_MILLIS;
                }
                if (MoquetteTest.CLIENT_UNSUBSCRIBE_BEFORE_DISCONNECT) {
                    for (String topic : topics) {
                        client.unsubscribe(topic);
                    }
                }
                LOGGER.debug("Disconnecting...");
                client.disconnect();
                LOGGER.debug("Disconnected.");
                if (!stop) {
                    sleep(MoquetteTest.CLIENT_DOWN_MILLIS);
                }
            } catch (MqttException ex) {
                LOGGER.error("Client Failed", ex);
            }
        }
        if (!lastWasClean) {
            try {
                // One last connect, to clean the session on the server.
                connectToServer(true);
                client.disconnect();
            } catch (MqttException ex) {
                LOGGER.error("Client Failed", ex);
            }
        }
    }

    private void connectToServer(final boolean cleanSession) throws MqttException {
        final MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(cleanSession);
        connOpts.setKeepAliveInterval(30);
        connOpts.setConnectionTimeout(30);
        connOpts.setMaxInflight(MoquetteTest.MAX_IN_FLIGHT_LISTENERS);
        LOGGER.info("Connecting with cleanSession={}", cleanSession);
        client.connect(connOpts);
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
