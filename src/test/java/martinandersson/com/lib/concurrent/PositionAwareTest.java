package martinandersson.com.lib.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Will test the ability of the {@code ConcurrentDequeManager} to report
 * position changes.
 * 
 * @author Martin Andersson (webmaster at martinandersson.com)
 */
public class PositionAwareTest
{
    private final long SLEEP_MS = 300;
    
    private final String KEY = "key";
    
    ConcurrentDequeManager<String, ElementActor> testee;
    
    ElementActor value1, value2;
    
    @BeforeClass
    public static void setupLogger()
    {
        Logger logger = Logger.getLogger(ConcurrentDequeManager.class.getName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
        logger.setLevel(Level.FINEST);
    }
    
    @Before
    public void setupTestee() {
        testee = new ConcurrentDequeManager<>(ForkJoinPool.commonPool());
        
        value1 = new ElementActor();
        value2 = new ElementActor();
    }
    
    @Test
    public void noExecutorServiceNoReport() throws InterruptedException
    {
        testee = new ConcurrentDequeManager<>();
        
        testee.addFirst(KEY, value1);
        testee.addFirst(KEY, value2);
        
        TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        
        assertEquals(0, value1.getPositions().count());
        assertEquals(0, value2.getPositions().count());
    }
    
    @Test
    public void actorInsertedBehind() throws InterruptedException
    {
        // No position changes for actor ahead
        
        testee.addFirst(KEY, value1);
        testee.addLast(KEY, value2);
        
        TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        
        assertEquals(0, value1.getPositions().count());
        assertEquals(0, value2.getPositions().count());
    }
    
    @Test
    public void actorInsertedAhead() throws InterruptedException
    {
        // Position changes for actor behind
        
        testee.addFirst(KEY, value1);
        testee.addFirst(KEY, value2);
        
        TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        
        assertEquals(0, value2.getPositions().count());
        
        // Moves from initial position 1 to new position 2:
        assertEquals(1, value1.getPositions().count());
        assertEquals(2, value1.getPositions().findFirst().getAsLong());
    }
    
    @Test
    public void actorBehindDrop() throws InterruptedException, ExecutionException
    {
        // No position changes for actor ahead
        
        testee.addFirst(KEY, value1);
        testee.addLast(KEY, value2);
        
        assertEquals(true, testee.removeFirstOccurance(KEY, value2).get());
        
        TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        
        assertEquals(0, value1.getPositions().count());
        assertEquals(0, value2.getPositions().count());
    }
    
    @Test
    public void actorAheadDrop() throws InterruptedException
    {
        // Position changes for actor behind
        
        testee.addFirst(KEY, value1);
        testee.addLast(KEY, value2);
        
        assertEquals(value1, testee.removeFirst(KEY).get());
        
        TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        
        assertEquals(0, value1.getPositions().count());
        
        // Moves from initial position 2 to new position 1:
        assertEquals(1, value2.getPositions().count());
        assertEquals(1, value2.getPositions().findFirst().getAsLong());
    }
}