package Lock;

import Clock.Scalar;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockHandler {

    final private String MESSAGETYPE_CLOCK = "AnnounceClock";
    final private String MESSAGETYPE_LOCK = "RequestLock";
    final private String MESSAGETYPE_UNLOCK = "AnnounceUnlock";

    final private Lock lock = new ReentrantLock();
    final private Map<Integer, Address> peers;
    private NettyMessagingService ms;

    private Clock.Vector clock;
    private PriorityQueue<Scalar> lockQueue = new PriorityQueue<Scalar>(Scalar::compareWithIdPriority);
    private boolean hasLock = false;
    private Queue<CompletableFuture<Void>> promises
            = new ArrayDeque<>(1);

    public LockHandler(int id, Map<Integer, Address> peers, NettyMessagingService ms, ScheduledExecutorService es){
        this.peers = peers;
        clock = new Clock.Vector(id, peers.keySet());
        this.ms = ms;
        enable(es);
    }

    public void enable(ScheduledExecutorService es){
        ms.registerHandler(MESSAGETYPE_CLOCK, (a,m) -> {
            Clock.Scalar scalar = Clock.Scalar.decode(ByteBuffer.wrap(m));
            handleClockRequest(a, scalar);
        }, es);
        ms.registerHandler(MESSAGETYPE_LOCK, (a,m) -> {
            handleLockRequest(a, Clock.Scalar.decode(ByteBuffer.wrap(m)));
        }, es);
        ms.registerHandler(MESSAGETYPE_UNLOCK, (a,m) -> {
            ByteBuffer buffer = ByteBuffer.wrap(m);
            handleUnlockRequest(a, Clock.Scalar.decode(buffer), Clock.Scalar.decode(buffer));
        }, es);
    }

    private void sendAsync(Address address, String type, byte[] buf, String log){
        System.out.println("[Sent][" + type + "|" + address + "] Log: " + log);
        ms.sendAsync(address, type, buf);

    }

    void handleClockRequest(Address address, Clock.Scalar scalar) {
        clock.updateWith(scalar);
        System.out.println("[Received]" + "[" + MESSAGETYPE_CLOCK + "|" + address + "]: " + scalar);
        acceptIfLockable();
    }

    void handleUnlockRequest(Address address, Clock.Scalar lockedScalar, Clock.Scalar scalar) {
        lock.lock();
        try {
            lockQueue.remove(lockedScalar);
            clock.updateWith(scalar);
            acceptIfLockable();
        } catch (Exception e) { e.printStackTrace(); } finally {
            lock.unlock();
        }
        System.out.println("[Received]" + "[" + MESSAGETYPE_UNLOCK + "|" + address + "]: " + scalar);
    }

    void handleLockRequest(Address address, Clock.Scalar scalar){
        byte[] buf = new byte[1];
        lock.lock();
        try {
            lockQueue.add(scalar);
            clock.updateWith(scalar);
            buf = clock.getLocal().encode();
        } catch (Exception e) { e.printStackTrace(); } finally {
            lock.unlock();
        }
        System.out.println("[Received]" + "[" + MESSAGETYPE_LOCK + "|" + address + "]: " + scalar);
        this.sendAsync(address, MESSAGETYPE_CLOCK, buf, "Clock: " + scalar);
    }

    public CompletableFuture<Void> lock(){
        byte[] buf;
        String log;
        CompletableFuture<Void> promise = new CompletableFuture<>();
        lock.lock();
        try {
            promises.add(promise);
            lockQueue.add(clock.getLocal());
            buf = clock.getLocal().encode();
            log = clock.getLocal().toString();
            clock.increment();
        } finally {
            lock.unlock();
        }
        for(Address peer : peers.values())
            sendAsync(peer, MESSAGETYPE_LOCK, buf, log);
        return promise;
    }

    public void unlock(){
        ByteBuffer buf = ByteBuffer.allocate(0);
        String log = "";
        lock.lock();
        try {
            if(!hasLock) return;
            byte[] queueScalar = lockQueue.peek().encode();
            log = lockQueue.peek().toString();
            lockQueue.remove(lockQueue.peek());
            clock.increment();
            byte[] scalar = clock.getLocal().encode();
            buf = ByteBuffer.allocate(queueScalar.length + scalar.length).put(queueScalar).put(scalar);
            promises.remove();
            hasLock = false;
        } catch (Exception e ) { e.printStackTrace();} finally {
            lock.unlock();
        }
        for(Address peer : peers.values())
            sendAsync(peer, MESSAGETYPE_UNLOCK, buf.array(), log);
    }

    void acceptIfLockable(){
        lock.lock();
        try{
            Scalar s = lockQueue.peek();
            if(!hasLock && s != null && s.id == clock.localId && clock.succeeds(s)) {
                hasLock = true;
                assert promises.peek() != null;
                promises.peek().complete((Void)null);
            }
        } catch (Exception e ) { e.printStackTrace();} finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "Lock.LockHandler{" + clock + " " + lockQueue + " " + (hasLock ? "L" : "U") + " " + "}";
    }
}
