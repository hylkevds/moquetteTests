package tests.moquettetests;

/**
 *
 * @author hylke
 */
public interface Listener extends Runnable {

    public long getRecvCount();

    public long getUnwantedCount();

    public void stop();
}
