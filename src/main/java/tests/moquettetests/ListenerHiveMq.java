package tests.moquettetests;

import com.hivemq.client.internal.mqtt.datatypes.MqttTopicFilterImpl;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttTopicFilter;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static tests.moquettetests.MoquetteTest.CLIENT_CLEAN_SESSION;

/**
 *
 * @author hylke
 */
class ListenerHiveMq implements Listener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListenerHiveMq.class.getName());
    public static final Charset UTF8 = Charset.forName("UTF-8");

    private final String clientId;
    private final String[] topics;
    private final List<MqttTopicFilter> topicFilters = new ArrayList<>();
    private final Mqtt3AsyncClient client;
    private final AtomicLong recvCount = new AtomicLong();
    private final AtomicLong recvUnwantedCount = new AtomicLong();
    private boolean stop = false;
    private Thread current;
    private boolean lastWasClean = false;
    private long nextSleep = MoquetteTest.CLIENT_LIVE_MILLIS_INITIAL;

    public ListenerHiveMq(String serverString, String clientId, String[] topics) throws URISyntaxException {
        this.clientId = clientId;
        this.topics = topics;
        for (String topic : topics) {
            topicFilters.add(MqttTopicFilterImpl.of(topic));
        }
        URI serverUrl = new URI(serverString);
        client = Mqtt3Client.builder()
                .identifier(clientId)
                .serverHost(serverUrl.getHost())
                .serverPort(serverUrl.getPort())
                .addDisconnectedListener((context) -> {
                    LOGGER.info("connectionLost");
                })
                .buildAsync();
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
            client.publishes(MqttGlobalPublishFilter.UNSOLICITED, t -> recvUnwantedCount.incrementAndGet());
            try {
                client.connectWith()
                        .cleanSession(shouldCleanSession)
                        .send().get();
            } catch (InterruptedException | ExecutionException ex) {
                LOGGER.error("Exception while unsubscribing.");
            }
            for (String topic : topics) {
                client.subscribeWith()
                        .topicFilter(topic)
                        .callback((t) -> {
                            recvCount.incrementAndGet();
                            if (t.getPayload().isEmpty()) {
                                LOGGER.error("Incorrect message: empty");
                            }
                            String msg = new String(t.getPayloadAsBytes(), UTF8);
                            if (!msg.startsWith("Last") || !msg.endsWith(MoquetteTest.MESSAGE)) {
                                LOGGER.error("Incorrect message: {}", msg);
                            }
                            LOGGER.trace("{} Received message on {}", clientId, t.getTopic());
                        })
                        .send();
            }
            LOGGER.info("Subscribed to {} topics with cleanSession={}.", topics.length, shouldCleanSession);
            if (!stop) {
                sleep(nextSleep);
                nextSleep = MoquetteTest.CLIENT_LIVE_MILLIS;
            }
            if (MoquetteTest.CLIENT_UNSUBSCRIBE_BEFORE_DISCONNECT) {
                try {
                    client.unsubscribeWith().addTopicFilters(topicFilters).send().get();
                } catch (InterruptedException | ExecutionException ex) {
                    LOGGER.error("Exception while unsubscribing.");
                }
            }
            client.disconnect();
            if (!stop) {
                sleep(MoquetteTest.CLIENT_DOWN_MILLIS);
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
