package tests.moquettetests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jab
 */
public class ProcessorHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorHelper.class);
    private static final long SHUTDOWN_WAIT_STEP_IN_MILLIS = 100;

    private static List<Processor> activities = new ArrayList<>();

    private ProcessorHelper() {
    }

    public static <T> ExecutorService createProcessors(int threadCount, BlockingQueue<T> queue, Consumer<T> consumer, String name) {
        ThreadGroup threadGroup = new ThreadGroup(name + "-TG");
        ExecutorService result = Executors.newFixedThreadPool(
                threadCount,
                (Runnable r) -> new Thread(threadGroup, r, name + "-T"));

        for (int i = 0; i < threadCount; i++) {
            Processor p = new Processor(queue, consumer, name);
            activities.add(p);
            result.submit(p);
        }
        return result;
    }

    public static void shutdownProcessors(ExecutorService executorService, BlockingQueue<?> queue, long timeout, TimeUnit timeUnit) {
        if (executorService != null) {
            executorService.shutdown();
            long timeoutInMillis = timeUnit.toMillis(timeout);
            /**
             * no more events can be added to the queue so simple wait till it's
             * empty.
             */
            try {
                while (queue != null && !queue.isEmpty() && timeoutInMillis > 0) {
                    Thread.sleep(SHUTDOWN_WAIT_STEP_IN_MILLIS);
                    timeoutInMillis -= SHUTDOWN_WAIT_STEP_IN_MILLIS;
                }
                executorService.shutdownNow();
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOGGER.debug("executoreService did not terminate in time");
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
            }
        }
    }

    public static boolean checkForHangingProcesses(long maxDuration) {
        long cutoff = System.currentTimeMillis() - maxDuration;
        for (Processor processor : activities) {
            long start = processor.getStart();
            if (start <= processor.getEnd()) {
                continue;
            }
            if (start < cutoff) {
                LOGGER.warn("Hanging Process: {}", processor.name);
                return true;
            }
        }
        return false;
    }

    static class Processor<T> implements Runnable {

        final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
        final BlockingQueue<T> queue;
        final Consumer<T> consumer;
        final String name;
        private long start = 0;
        private long end = 0;

        private Processor(BlockingQueue<T> queue, Consumer<T> consumer) {
            this(queue, consumer, null);
        }

        private Processor(BlockingQueue<T> queue, Consumer<T> consumer, String name) {
            if (queue == null) {
                throw new IllegalArgumentException("queue must be non-null");
            }
            if (consumer == null) {
                throw new IllegalArgumentException("handler must be non-null");
            }
            if (name == null || name.isEmpty()) {
                this.name = getClass().getName();
            } else {
                this.name = name;
            }
            this.queue = queue;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            LOGGER.debug("starting {}-Thread", name);
            while (!Thread.currentThread().isInterrupted()) {
                T event;
                try {
                    event = queue.take();
                    start = System.currentTimeMillis();
                    consumer.accept(event);
                } catch (InterruptedException ex) {
                    LOGGER.debug(name + " interrupted");
                    LOGGER.trace(name + " interrupted", ex);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    LOGGER.warn("Exception while executing " + name, ex);
                } finally {
                    end = System.currentTimeMillis();
                }
            }
            LOGGER.debug("exiting {}-Thread", name);
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return end;
        }

    }
}
