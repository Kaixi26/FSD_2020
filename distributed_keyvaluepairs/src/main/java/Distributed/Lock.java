package Distributed;

import Clock.Scalar;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Lock {
    final private String MESSAGETYPE_CLOCK;
    final private String MESSAGETYPE_LOCK;
    final private String MESSAGETYPE_UNLOCK;
    final boolean debug;
    final int id;
    final Map<Integer, Address> peers;
    final NettyMessagingService ms;
    final ExecutorService es = Executors.newScheduledThreadPool(1);
    final private PriorityQueue<Scalar> lockQueue
            = new PriorityQueue<>(Scalar::compareWithIdPriority);
    final private Queue<CompletableFuture<Scalar>> lockRequests = new ArrayDeque<>(1);

    private Clock.Vector vectorClock;
    private boolean hasLock = false;

    public Lock(int id, Map<Integer, Address> peers, NettyMessagingService ms, String messagePrefix, boolean debug){
        this.MESSAGETYPE_CLOCK  = messagePrefix + "AnnounceClock";
        this.MESSAGETYPE_LOCK   = messagePrefix + "RequestLock";
        this.MESSAGETYPE_UNLOCK = messagePrefix + "AnnounceUnlock";
        this.id = id;
        this.peers = peers;
        this.ms = ms;
        this.vectorClock = new Clock.Vector(id, peers.keySet());
        this.debug = debug;
    }

    public Lock start(){
        ms.registerHandler(MESSAGETYPE_CLOCK, (a,m) -> {
            try {
                Clock.Scalar scalar = Clock.Scalar.decode(ByteBuffer.wrap(m));
                handleClockRequest(a, scalar);
            } catch (Exception e) { e.printStackTrace(); }
        }, es);
        ms.registerHandler(MESSAGETYPE_LOCK, (a,m) -> {
            try {
                handleLockRequest(a, Clock.Scalar.decode(ByteBuffer.wrap(m)));
            } catch (Exception e) { e.printStackTrace(); }
        }, es);
        ms.registerHandler(MESSAGETYPE_UNLOCK, (a,m) -> {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(m);
                handleUnlockRequest(a, Clock.Scalar.decode(buffer), Clock.Scalar.decode(buffer));
            } catch (Exception e){ e.printStackTrace(); }
        }, es);
        return this;
    }

    private void log(String log) {
        if(debug)
            System.out.println(log);
    }

    private void handleClockRequest(Address address, Clock.Scalar scalar) {
        vectorClock.updateWith(scalar);
        log("[R]" + "[" + MESSAGETYPE_CLOCK + "|" + address + "]: " + scalar + " " + this);
        tryLockNext();
    }

    private void handleUnlockRequest(Address address, Clock.Scalar lockedScalar, Clock.Scalar scalar) {
        lockQueue.remove(lockedScalar);
        vectorClock.updateWith(scalar);
        log("[R]" + "[" + MESSAGETYPE_UNLOCK + "|" + address + "]: " + scalar + " " + this);
        tryLockNext();
    }

    private void handleLockRequest(Address address, Clock.Scalar scalar){
        lockQueue.add(scalar);
        vectorClock.updateWith(scalar);
        log("[R]" + "[" + MESSAGETYPE_LOCK + "|" + address + "]: " + scalar + " " + this);
        this.send(address, MESSAGETYPE_CLOCK, vectorClock.getLocal().encode(), "Clock: " + scalar);
    }

    private void send(Address address, String type, byte[] buf, String log) {
        ms.sendAsync(address, type, buf)
                .thenAcceptAsync(_v -> {
                    log("[S][" + type + "|" + address + "]: " + log);
                });
    }

    private void sendLock(Clock.Scalar clock){
        byte[] buf = clock.encode();
        String log =  vectorClock.getLocal() + " " + this;
        vectorClock.increment();
        for(Address peer : peers.values())
            send(peer, MESSAGETYPE_LOCK, buf, log);
    }

    private void sendUnlock(Clock.Scalar clock){
        hasLock = false;
        byte[] queueScalar = clock.encode();
        byte[] scalar = vectorClock.getLocal().encode();
        ByteBuffer buf = ByteBuffer.allocate(queueScalar.length + scalar.length)
                .put(queueScalar)
                .put(scalar);
        for(Address peer : peers.values())
            send(peer, MESSAGETYPE_UNLOCK, buf.array(), "Unlocked " + clock.toString());
        tryLockNext();
    }

    private void tryLockNext(){
        if (!hasLock && lockQueue.size() > 0
                && lockQueue.peek().id == id
                && vectorClock.succeeds(lockQueue.peek())){
            lockRequests.poll().complete(lockQueue.poll());
            hasLock = true;
        }
    }

    public CompletableFuture<CompletableFuture<Void>> whenLocked(){
        CompletableFuture<CompletableFuture<Void>> ret = new CompletableFuture<>();
        CompletableFuture<Void> unlocker = new CompletableFuture<>();
        CompletableFuture<Clock.Scalar> completableFuture = new CompletableFuture<>();
        es.execute(() -> {
            lockRequests.add(completableFuture);
            lockQueue.add(vectorClock.getLocal());
            sendLock(vectorClock.getLocal());
        });
        completableFuture.thenAcceptAsync(clock -> {
            ret.complete(unlocker);
            unlocker.thenAcceptAsync(_v -> es.execute(() -> sendUnlock(clock)));
        });
        return ret;
    }
    public String toString() {
        return "Lock{" + vectorClock + " " + lockQueue + " " + " " + hasLock + " " + lockRequests + "}";
    }

}

