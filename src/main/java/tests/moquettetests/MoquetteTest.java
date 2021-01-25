package tests.moquettetests;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Hylke van der Schaaf
 */
public class MoquetteTest {

    public static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MoquetteTest.class);
    private static final int QOS = 2;
    private static final String BROKER_URL = "tcp://localhost:1883";
    private static final String TOPIC_PREFIX = ""; //some/deep(er)/path/";
    private static final String TOPIC_POSTFIX = ""; //?$select=result,phenomenonTime";
    private static final long CLIENT_LIVE_MILLIS = 2000;
    private static final long CLIENT_DOWN_MILLIS = 100;
    private static final long PUBLISHER_SLEEP_MILLIS = 100;
    private static final long TOPIC_COUNT = 20000;

    private Server broker;
    private MqttClient client;
    private final String clientId = "TestClient (" + UUID.randomUUID() + ")";
    private final int maxInFlight = 9999;
    private final int threadCountPublish = 1;
    private final int threadCountListen = 1;
    /**
     * The number of milliseconds a worker is allowed to not work before we
     * complain.
     */
    private final long cutoff = 200;

    private List<Publisher> publishers = new ArrayList<>();
    private List<Listener> listeners = new ArrayList<>();
    private List<String> topicList = new ArrayList<>();

    private BlockingQueue<String> messageQueue;
    private ExecutorService executorService;

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checker;
    private int failedCount = 0;

    public static final String MESSAGE = "test";

    private static class Publisher implements Runnable {

        /**
         * The time the last message was sent. No need for synchronisation,
         * precision is not important.
         */
        private long lastMessage = 0;
        private boolean stop = false;
        private final MqttClient client;
        private final String topic;
        /**
         * Flag to indicate we think it is working, so we don't keep complaining
         * about the same worker.
         */
        private boolean working = true;

        public Publisher(MqttClient client, String topic) {
            this.client = client;
            this.topic = topic;
        }

        @Override
        public void run() {
            LOGGER.info("Publishing on: {}", topic);
            while (!stop) {
                String message = "Last: " + lastMessage + MoquetteTest.MESSAGE;
                try {
                    if (client.isConnected()) {
                        client.publish(topic, message.getBytes(UTF8), 0, false);
                    } else {
                        return;
                    }
                } catch (MqttException ex) {
                    LOGGER.error("Failed to send message.", ex);
                }
                if (PUBLISHER_SLEEP_MILLIS > 0) {
                    try {
                        Thread.sleep(PUBLISHER_SLEEP_MILLIS);
                    } catch (InterruptedException ex) {
                        LOGGER.error("Interrupted.", ex);
                    }
                }
                lastMessage = System.currentTimeMillis();
            }
        }

        public void stop() {
            stop = true;
        }

        public long getLastMessage() {
            return lastMessage;
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

    }

    private static class Listener implements Runnable {

        private final String clientId;
        private final String[] topics;

        private final MqttClient client;
        private final AtomicLong recvCount = new AtomicLong();
        private boolean stop = false;

        public Listener(String serverUrl, String clientId, String[] topics) throws MqttException {
            this.clientId = clientId;
            this.topics = topics;
            this.client = new MqttClient(serverUrl, clientId);

        }

        public void stop() {
            stop = true;
        }

        public long getRecvCount() {
            return recvCount.get();
        }

        @Override
        public void run() {
            LOGGER.info("Listening on {} topics", topics.length);
            while (!stop) {
                try {
                    client.connect();
                    for (String topic : topics) {
                        client.subscribe(topic, QOS,
                                (String msgTopic, MqttMessage message) -> {
                                    recvCount.incrementAndGet();
                                    LOGGER.trace("{} Received message on {}", clientId, msgTopic);
                                }
                        );
                    }
                    sleep(CLIENT_LIVE_MILLIS);
                    client.unsubscribe(topics);
                    client.disconnect();
                    sleep(CLIENT_DOWN_MILLIS);
                } catch (MqttException ex) {
                    LOGGER.error("Client Failed", ex);
                }
            }
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException ex) {
            }
        }
    }

    public MoquetteTest() {
        messageQueue = new ArrayBlockingQueue<>(1000);
        executorService = ProcessorHelper.createProcessors(10, messageQueue, m -> sendMessage(m), "MessageSender");
    }

    public void sendMessage(String message) {
        try {
            client.publish("test", message.getBytes(UTF8), QOS, false);
        } catch (MqttException ex) {
            LOGGER.error("Failed to send message.", ex);
        }
    }

    public void stopPublishers() {
        if (checker != null) {
            if (!checker.cancel(true)) {
                LOGGER.info("Failed to cancel checker task.");
            }
            checker = null;
        }
        for (Publisher worker : publishers) {
            worker.stop();
        }
        publishers.clear();
    }

    public void stopListeners() {
        for (Listener worker : listeners) {
            worker.stop();
        }
        listeners.clear();
    }

    public void createPublisers() {
        if (!publishers.isEmpty()) {
            stopPublishers();
        }
        for (int i = 0; i < threadCountPublish; i++) {
            Publisher worker = new Publisher(client, topicList.get(i));
            publishers.add(worker);
            LOGGER.info("Created worker {}.", worker.getTopic());
        }
    }

    public void createListeners() throws MqttException {
        if (!listeners.isEmpty()) {
            stopListeners();
        }
        for (int i = 0; i < threadCountListen; i++) {
            String listenerId = "Listener-" + i;
            Listener worker = new Listener(BROKER_URL, listenerId, topicList.toArray(new String[topicList.size()]));
            listeners.add(worker);
            LOGGER.info("Created Listener {}.", listenerId);
        }
    }

    public void startWorkers() {
        for (Publisher worker : publishers) {
            new Thread(worker).start();
        }
        if (checker != null) {
            if (!checker.cancel(true)) {
                LOGGER.info("Failed to cancel checker task.");
            }
        }
        checker = executor.scheduleAtFixedRate(this::checkWorkers, 500, 500, TimeUnit.MILLISECONDS);
    }

    public void startListeners() {
        for (Listener worker : listeners) {
            new Thread(worker).start();
        }
    }

    public void checkWorkers() {
        long now = System.currentTimeMillis();
        long cutOff = now - cutoff;
        for (Publisher worker : publishers) {
            if (worker.isWorking()) {
                if (worker.getLastMessage() < cutOff) {
                    failedCount++;
                    worker.setWorking(false);
                    LOGGER.warn("Worker {} is not working. Now {} stopped.", worker.getTopic(), failedCount);
                }
            } else {
                if (worker.getLastMessage() > cutOff) {
                    failedCount--;
                    worker.setWorking(true);
                    LOGGER.warn("Worker {} seems to have resumed working. Now {} stopped.", worker.getTopic(), failedCount);
                }
            }
        }
        long total = 0;
        for (Listener listener : listeners) {
            total += listener.getRecvCount();
        }
        LOGGER.info("Received {} messages on all topics", total);
    }

    public void startServer() {
        broker = new Server();
        IConfig config = new MemoryConfig(new Properties());

        config.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        config.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, Boolean.TRUE.toString());

        try {
            broker.startServer(config);
            try {
                client = new MqttClient(BROKER_URL, clientId, new MemoryPersistence());
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                connOpts.setKeepAliveInterval(30);
                connOpts.setConnectionTimeout(30);
                connOpts.setMaxInflight(maxInFlight);
                LOGGER.info("paho-client connecting to broker: " + broker);
                try {
                    client.connect(connOpts);
                    client.subscribe("#");
                    LOGGER.info("paho-client connected to broker");
                } catch (MqttException ex) {
                    LOGGER.error("Could not connect to MQTT server.", ex);
                }
            } catch (MqttException ex) {
                LOGGER.error("Could not create MQTT Client.", ex);
            }
        } catch (IOException ex) {
            LOGGER.error("Could not start MQTT server.", ex);
        }
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void stopServer() {
        executor.shutdown();
        if (client != null && client.isConnected()) {
            try {
                client.disconnectForcibly();
            } catch (MqttException ex) {
                LOGGER.debug("exception when forcefully disconnecting MQTT client", ex);
            }
            try {
                client.close(true);
            } catch (MqttException ex) {
                LOGGER.debug("exception when forcefully disconnecting MQTT client", ex);
            }
        }
        if (broker != null) {
            broker.stopServer();
        }
        List<Runnable> shutdownNow = executor.shutdownNow();
        LOGGER.info("Stopped checker. {} tasks still running.", shutdownNow.size());
    }

    public void work() throws UnsupportedEncodingException, IOException, MqttException {
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topicList.add(TOPIC_PREFIX + "Client-" + i + TOPIC_POSTFIX);
        }
        startServer();
        createPublisers();
        createListeners();
        startWorkers();
        startListeners();

        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
            LOGGER.warn("Press Enter to exit.");
            input.read();
            LOGGER.warn("Exiting...");
            stopListeners();
            stopPublishers();
            stopServer();
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.io.UnsupportedEncodingException
     * @throws org.eclipse.paho.client.mqttv3.MqttException
     */
    public static void main(String[] args) throws UnsupportedEncodingException, IOException, MqttException {
        MoquetteTest pahoTest = new MoquetteTest();
        pahoTest.work();
    }

}
