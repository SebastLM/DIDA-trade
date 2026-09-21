package didatrade.server;

import java.util.*;



public class DebugModeHandler {
    
    private boolean frozen = false;
    private final Object freezeLock = new Object();

    public void doDebugMode(int mode) {
        switch (mode) {
            case 0 -> crash();
            case 1 -> freeze();
            case 2 -> unFreeze();
            case 3 -> slowMode();
            case 4 -> slowModeOff();
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

    public void awaitUnfrozen() {
        synchronized (freezeLock) {
            while (frozen) {
                try {
                    freezeLock.wait();
                } catch (InterruptedException e) {
                    System.err.println("thread interrupted");
                    return;
                }
            }
        }
    }

    public void slowMode() {
        // TODO
    }

    public void slowModeOff() {
        // TODO
    }

}