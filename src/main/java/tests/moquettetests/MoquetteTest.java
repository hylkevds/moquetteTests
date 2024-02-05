package tests.moquettetests;

import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
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

    public static enum YES_NO_ALTERNATE {
        YES,
        NO,
        ALTERNATE
    }
    public static final Charset UTF8 = Charset.forName("UTF-8");

    /**
     * The logger for this class.
     */
    public static final Logger LOGGER = LoggerFactory.getLogger(MoquetteTest.class);
    public static final int QOS_LISTEN = 2;
    public static final int QOS_PUBLISH = 2;
    public static final String BROKER_URL = "tcp://127.0.0.1:1883";
    public static final String TOPIC_PREFIX = "Datastreams(";
    public static final String TOPIC_POSTFIX = ")/Observations";

    public static final boolean USE_PAHO_CLIENT = true;
    public static final boolean USE_HIVEMQ_CLIENT = false;
    /**
     * Clients connect, subscript, listen for this long, then unsubscribe.
     */
    public static final long CLIENT_LIVE_MILLIS = 30_000;
    public static final long CLIENT_LIVE_MILLIS_INITIAL = 30_000;
    /**
     * After unsubscribing, clients sleep for this long, and start over.
     */
    public static final long CLIENT_DOWN_MILLIS = 10;

    public static final YES_NO_ALTERNATE CLIENT_CLEAN_SESSION = YES_NO_ALTERNATE.ALTERNATE;

    public static final boolean CLIENT_UNSUBSCRIBE_BEFORE_DISCONNECT = true;

    public static final boolean CLIENT_RANDOMISE_IDS = true;

    /**
     * Publish directly using moquette internal API?
     */
    public static final boolean PUBLISH_DIRECT = false;
    /**
     * Publish this many messages in one go.
     */
    public static final long PUBLISHER_BATCH_COUNT = 500;
    /**
     * Slowly increase the publish batch count with this amount until
     * {@link #PUBLISHER_BATCH_COUNT} is reached.
     */
    public static final long PUBLISHER_BATCH_COUNT_INCREMENT = 50;
    /**
     * Then sleep for this long.
     */
    public static final long PUBLISHER_SLEEP_MILLIS = 2_000;

    /**
     * How long to wait before the start the publishers.
     */
    public static final long PUBLISHER_START_DELAY_MILLIS = 5_000;
    /**
     * How long to wait before the start of the next publisher.
     */
    public static final long PUBLISHER_RAMP_UP_DELAY_MILLIS = 200;

    private static final int WORKER_CHECK_INTERVAL = 1_000;

    public static final int MAX_IN_FLIGHT_LISTENERS = 9999;
    public static final int MAX_IN_FLIGHT_PUBLISHERS = 40;
    public static final long TOPIC_COUNT = 20;
    public static final int H2_AUTO_SAVE_INTERVAL = 1;

    private Server broker;
    private final int threadCountPublish = 2;
    private final int threadCountListen = 20;

    /**
     * The number of milliseconds a worker is allowed to not work before we
     * complain.
     */
    private final long cutoff = PUBLISHER_SLEEP_MILLIS + 500;

    private final List<Publisher> publishers = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final List<String> topicList = new ArrayList<>();

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> checker;
    private int failedCount = 0;

    public static final String MESSAGE = "test";

    public MoquetteTest() {
        System.out.println("\n\nTest\n\n");
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

    public void createPublisers() throws MqttException, URISyntaxException {
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

    public void createListeners() throws MqttException, URISyntaxException {
        if (!listeners.isEmpty()) {
            stopListeners();
        }
        boolean lastWasPaho = false;
        for (int i = 0; i < threadCountListen; i++) {
            String listenerId = "Listener-" + i;
            Listener worker;
            if (USE_PAHO_CLIENT && !lastWasPaho || !USE_HIVEMQ_CLIENT) {
                worker = new ListenerPaho(BROKER_URL, listenerId, topicList.toArray(new String[topicList.size()]));
                lastWasPaho = true;
            } else {
                worker = new ListenerHiveMq(BROKER_URL, listenerId, topicList.toArray(new String[topicList.size()]));
                lastWasPaho = false;
            }
            listeners.add(worker);
            LOGGER.info("Created Listener {}.", listenerId);
        }
    }

    public void startPublishers() {
        sleep(PUBLISHER_START_DELAY_MILLIS);
        for (Publisher worker : publishers) {
            sleep(PUBLISHER_RAMP_UP_DELAY_MILLIS);
            new Thread(worker).start();
        }
    }

    public void startListeners() {
        if (checker != null) {
            if (!checker.cancel(true)) {
                LOGGER.info("Failed to cancel checker task.");
            }
        }
        checker = executor.scheduleAtFixedRate(this::checkWorkers, 0, WORKER_CHECK_INTERVAL, TimeUnit.MILLISECONDS);
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
                        LOGGER.warn("Publisher {} is not working. Now {} stopped.", worker.getTopic(), failedCount);
                    }
                } else {
                    if (worker.getLastMessage() > cutOff) {
                        failedCount--;
                        worker.setWorking(true);
                        LOGGER.warn("Publisher {} seems to have resumed working. Now {} stopped.", worker.getTopic(), failedCount);
                    }
                }
            }
        }
        long totalRecv = 0;
        long totalRecvOdd = 0;
        for (Listener listener : listeners) {
            totalRecv += listener.getRecvCount();
            totalRecvOdd += listener.getUnwantedCount();
        }
        long diff = (long) (Math.floor(totalRecv + totalRecvOdd) - totalSent * threadCountListen);
        StringBuilder failCounts = new StringBuilder();
        StringBuilder sizeCounts = new StringBuilder();
//        for (int i = 0; i < PostOffice.QUEUE_FAILURES.length; i++) {
//            failCounts.append(" ").append(PostOffice.QUEUE_FAILURES[i].get());
//            sizeCounts.append(" ").append(PostOffice.SESSION_QUEUES[i].remainingCapacity());
//        }
        LOGGER.info("Sent/Received {} / {} + {} messages ({}) in {} batches. {} / {} ({}, {})",
                totalSent, totalRecv, totalRecvOdd, diff, totalBatches,
                sizeCounts, failCounts, 0, 0);
        //PostOffice.FAILED_PUBLISHES.packetsMap.size(), PostOffice.SUBSCRIPTIONS.size());
    }

    public void startServer() throws IOException {
        broker = new Server();

        IConfig config = new MemoryConfig(new Properties());
        config.setProperty(IConfig.ALLOW_ANONYMOUS_PROPERTY_NAME, Boolean.TRUE.toString());
        config.setProperty(IConfig.BUFFER_FLUSH_MS_PROPERTY_NAME, "0");
        config.setProperty(IConfig.NETTY_MAX_BYTES_PROPERTY_NAME, "200000");
        config.setProperty(IConfig.SESSION_QUEUE_SIZE, "1024");
        config.setProperty(IConfig.PERSISTENT_QUEUE_TYPE_PROPERTY_NAME, "segmented");
        config.setProperty(IConfig.PERSISTENT_CLIENT_EXPIRATION_PROPERTY_NAME, "30s");
        final Path tempDir = Files.createTempDirectory("moquette");
        config.setProperty(IConfig.DATA_PATH_PROPERTY_NAME, tempDir.resolve("moquette").toFile().getAbsolutePath());

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

    public void work() throws UnsupportedEncodingException, IOException, MqttException, URISyntaxException {
        for (int i = 0; i < TOPIC_COUNT; i++) {
            topicList.add(TOPIC_PREFIX + i + TOPIC_POSTFIX);
        }
        startServer();
        createPublisers();
        createListeners();
        startListeners();
        startPublishers();

        try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in, "UTF-8"))) {
            LOGGER.warn("Press Enter to stop publishers.");
            input.read();
            LOGGER.warn("Stopping Publishers...");
            stopPublishers();

            LOGGER.warn("Press Enter to stop listeners.");
            input.read();
            LOGGER.warn("Stopping Listeners...");
            stopListeners();

            LOGGER.warn("Press Enter to exit.");
            input.read();
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
    public static void main(String[] args) throws UnsupportedEncodingException, IOException, MqttException, URISyntaxException {
        MoquetteTest pahoTest = new MoquetteTest();
        pahoTest.work();
    }

}
