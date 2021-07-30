package tests.moquettetests;

/**
 *
 * @author hylke
 */
public interface Listener extends Runnable {

    public long getRecvCount();

    public void stop();
}
