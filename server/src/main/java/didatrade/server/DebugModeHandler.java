package didatrade.server;

import java.util.*;
import java.util.concurrent.TimeUnit;


public class DebugModeHandler {
    
    private static final int MIN_TIMEOUT = 1;
    private static final int MAX_TIMEOUT = 5;
    
    private boolean frozen = false;
    private final Object freezeLock = new Object();
    
    Random r = new Random();
    private int random_timeout = 0;

    public void doDebugMode(int mode) {
        switch (mode) {
            case 1 -> crash();
            case 2 -> freeze();
            case 3 -> unFreeze();
            case 4 -> slowMode();
            case 5 -> slowModeOff();
        }
    }

    public boolean getFreezeState() {
        return frozen;
    }

    public void crash() {
        // TODO
        /*
          System.exit();
          // how does the commandService know we exited, do we implement a time out there, do we send response before exiting?
        */
    }

    public void freeze() {
        synchronized (freezeLock) {
            frozen = true;
        }
    }

    public void unFreeze() {
        synchronized (freezeLock) {
            frozen = false;
            freezeLock.notifyAll();
        }
    }

    public void verifyDebug() {
        if (frozen == true) {
            awaitUnfrozen();
        } else if (random_timeout != 0) {
            awaitTimeOut();
        }
    }

    public void awaitUnfrozen() {
        synchronized (freezeLock) {
            while (frozen) {
                try {
                    freezeLock.wait();
                } catch (InterruptedException e) {
                    System.err.println("thread interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void slowMode() {
        random_timeout = r.nextInt(MAX_TIMEOUT - MIN_TIMEOUT + 1) + MIN_TIMEOUT;
    }

    public void slowModeOff() {
        random_timeout = 0;
    }

    public void awaitTimeOut() {
        try {
            Thread.sleep(random_timeout * 1000);
        } catch (InterruptedException e) {
            System.err.println("thread interrupted");
            Thread.currentThread().interrupt();
        }
    }

}