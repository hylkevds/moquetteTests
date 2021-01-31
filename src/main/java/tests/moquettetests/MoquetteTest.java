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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.eclipse.paho.client.mqttv3.MqttException;
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
    public static final Logger LOGGER = LoggerFactory.getLogger(MoquetteTest.class);
    public static final int QOS_LISTEN = 1;
    public static final int QOS_PUBLISH = 2;
    public static final String BROKER_URL = "tcp://127.0.0.1:1883";
    public static final String TOPIC_PREFIX = "Datastreams(";
    public static final String TOPIC_POSTFIX = ")/Observations";

    /**
     * Clients connect, subscript, listen for this long, then unsubscribe.
     */
    public static final long CLIENT_LIVE_MILLIS = 60 * 60 * 1000;
    /**
     * After unsubscribing, clients sleep for this long, and start over.
     */
    public static final long CLIENT_DOWN_MILLIS = 1;

    /**
     * Publish directly using moquette internal API?
     */
    public static final boolean PUBLISH_DIRECT = true;
    /**
     * Publish this many messages in one go.
     */
    public static final long PUBLISHER_BATCH_COUNT = 100;
    /**
     * Then sleep for this long.
     */
    public static final long PUBLISHER_SLEEP_MILLIS = 1_000;

    /**
     * How long to wait before the start the publishers.
     */
    public static final long PUBLISHER_START_DELAY_MILLIS = 1_000;
    /**
     * How long to wait before the start of the next publisher.
     */
    public static final long PUBLISHER_RAMP_UP_DELAY_MILLIS = 1;

    public static final int MAX_IN_FLIGHT = 9999;
    public static final long TOPIC_COUNT = 20;
    public static final int H2_AUTO_SAVE_INTERVAL = 1;

    private Server broker;
    private final int threadCountPublish = 1;
    private final int threadCountListen = 1;

    /**
     * The number of milliseconds a worker is allowed to not work before we
     * complain.
     */
    private final long cutoff = PUBLISHER_SLEEP_MILLIS + 100;

    private final List<Publisher> publishers = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final List<String> topicList = new ArrayList<>();

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checker;
    private int failedCount = 0;

    public static final String MESSAGE = "test";

    public MoquetteTest() {
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

    public void createPublisers() throws MqttException {
        if (!publishers.isEmpty()) {
            stopPublishers();
        }
        for (int i = 0; i < threadCountPublish; i++) {
            String publisherId = "Publisher-" + i;
            Publisher worker;
            if (PUBLISH_DIRECT) {
                worker = new Publisher(broker, publisherId, topicList.get(i));
            } else {
                worker = new Publisher(BROKER_URL, publisherId, topicList.get(i));
            }
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
        sleep(PUBLISHER_START_DELAY_MILLIS);
        if (checker != null) {
            if (!checker.cancel(true)) {
                LOGGER.info("Failed to cancel checker task.");
            }
        }
        checker = executor.scheduleAtFixedRate(this::checkWorkers, 500, 500, TimeUnit.MILLISECONDS);
        for (Publisher worker : publishers) {
            sleep(PUBLISHER_RAMP_UP_DELAY_MILLIS);
            new Thread(worker).start();
        }
    }

    public void startListeners() {
        for (Listener worker : listeners) {
            new Thread(worker).start();
        }
    }

    public void checkWorkers() {
        long now = System.currentTimeMillis();
        long cutOff = now - cutoff;
        long totalSent = 0;
        long totalBatches = 0;
        for (Publisher worker : publishers) {
            totalSent += worker.getSentCount();
            totalBatches += worker.getBatchCount();
            if (!worker.isStopped()) {
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
        }
        long totalRecv = 0;
        for (Listener listener : listeners) {
            totalRecv += listener.getRecvCount();
        }
        long diff = totalRecv - totalSent;
        LOGGER.info("Sent/Received {} / {} messages ({}) in {} batches", totalSent, totalRecv, diff, totalBatches);
    }

    public void startServer() {
        broker = new Server();

        IConfig config = new MemoryConfig(new Properties());
        config.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        config.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, Boolean.TRUE.toString());
        final Path storePath = Paths.get(BrokerConstants.DEFAULT_MOQUETTE_STORE_H2_DB_FILENAME);
        if (storePath.toFile().exists()) {
            storePath.toFile().delete();
        }
        String defaultPersistentStore = storePath.toString();
        config.setProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME, defaultPersistentStore);
        config.setProperty(BrokerConstants.AUTOSAVE_INTERVAL_PROPERTY_NAME, Integer.toString(H2_AUTO_SAVE_INTERVAL));

        try {
            broker.startServer(config);
        } catch (IOException ex) {
            LOGGER.error("Could not start MQTT server.", ex);
        }
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    public void stopServer() {
        executor.shutdown();
        if (broker != null) {
            broker.stopServer();
        }
        List<Runnable> shutdownNow = executor.shutdownNow();
        LOGGER.info("Stopped checker. {} tasks still running.", shutdownNow.size());
    }

    public void work() throws UnsupportedEncodingException, IOException, MqttException {
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topicList.add(TOPIC_PREFIX + i + TOPIC_POSTFIX);
        }
        startServer();
        createPublisers();
        createListeners();
        startListeners();
        startWorkers();

        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
            LOGGER.warn("Press Enter to exit.");
            input.read();
            LOGGER.warn("Stopping Listeners...");
            stopListeners();
            LOGGER.warn("Stopping Publishers...");
            stopPublishers();
            sleep(1000);
            LOGGER.warn("Stopping Server...");
            stopServer();
        }
    }

    private void sleep(long time) {
        if (time <= 0) {
            return;
        }
        try {
            Thread.sleep(time);
        } catch (InterruptedException ex) {
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
