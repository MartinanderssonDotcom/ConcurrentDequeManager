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
    private final static ThreadLocal<AtomicInteger> counter = ThreadLocal.withInitial(AtomicInteger::new);
    
    private final AtomicBoolean isInCallback = new AtomicBoolean();
    
    private final List<Long> positions = new LinkedList<>();
    
    private final String value;
    
    public ElementActor() {
        value = String.valueOf(Thread.currentThread().getId()) + "-" + counter.get().incrementAndGet();
    }
    
    @Override
    public void setPosition(long position)
    {
        if (!isInCallback.compareAndSet(false, true))
            throw new ConcurrentModificationException();
        
        positions.add(position);
        
        if (!isInCallback.compareAndSet(true, false))
            throw new ConcurrentModificationException();
    }
    
    public LongStream getPositions() {
        return positions.stream().mapToLong(l -> l);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
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
        
        return this.value.equals(((ElementActor) obj).value);
    }

    @Override
    public String toString() {
        return ElementActor.class.getSimpleName() + "[ value=" + value + " ]";
    }
}