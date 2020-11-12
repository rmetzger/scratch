package de.robertmetzger;

public class JVMNotShuttingDown {

    public static void main(String[] args) throws InterruptedException {

        Thread hook = new Thread(() -> {
            Object l = new Object();
            synchronized (l) {
                try {
                    l.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException("nooo!", e);
                }
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);

        Thread.sleep(500000);
    }
}
