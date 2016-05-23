package martinandersson.com.lib.concurrent;

import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

/**
 * Will remember each reported position and fail-fast on concurrent callback
 * invocations.
 * 
 * @author Martin Andersson (webmaster at martinandersson.com)
 */
public class ElementActor implements ConcurrentDequeManager.PositionAware
{
    private final static ThreadLocal<AtomicInteger> SEQ
            = ThreadLocal.withInitial(AtomicInteger::new);
    
    private final AtomicBoolean isInCallback = new AtomicBoolean();
    
    private final List<Long> positions = new LinkedList<>();
    
    private final String val;
    
    
    public ElementActor() {
        val = String.valueOf(Thread.currentThread().getId()) + "-" + SEQ.get().incrementAndGet();
    }
    
    
    @Override
    public void newPosition(long position) {
        if (!isInCallback.compareAndSet(false, true)) {
            throw new ConcurrentModificationException();
        }
        
        try {
            positions.add(position);
        }
        finally {
            isInCallback.set(false);
        }
    }
    
    public LongStream positions() {
        return positions.stream().mapToLong(l -> l);
    }
    
    public long countReports() {
        return positions().count();
    }
    
    
    @Override
    public int hashCode() {
        return val.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        
        if (obj == null)
            return false;
        
        if (ElementActor.class != obj.getClass())
            return false;
        
        return this.val.equals(((ElementActor) obj).val);
    }
    
    @Override
    public String toString() {
        return ElementActor.class.getSimpleName() + "[ val=" + val + " ]";
    }
}