package martinandersson.com.lib.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Will test the ability of the {@code ConcurrentDequeManager} to report
 * position changes.
 * 
 * @author Martin Andersson (webmaster at martinandersson.com)
 */
public class PositionAwareTest
{
    private final long SLEEP_MS = 100;
    
    private final String KEY = "key";
    
    
    
    ConcurrentDequeManager<String, ElementActor> testee;
    
    ElementActor elem1,
                 elem2;
    
    
    
    @BeforeClass
    public static void __beforeClass() {
        Logger logger = Logger.getLogger(ConcurrentDequeManager.class.getName());
        ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        logger.addHandler(handler);
        logger.setLevel(Level.FINEST);
    }
    
    @Before
    public void __before() {
        testee = new ConcurrentDequeManager<>(ForkJoinPool.commonPool());
        
        elem1 = new ElementActor();
        elem2 = new ElementActor();
    }
    
    
    
    @Test
    public void test_noExecutorServiceNoReport() {
        testee = new ConcurrentDequeManager<>();
        
        testee.addFirst(KEY, elem1);
        testee.addFirst(KEY, elem2);
        
        sleep();
        
        assertEquals(0, elem1.countReports());
        assertEquals(0, elem2.countReports());
    }
    
    @Test
    public void test_elemInsertedLast_noPositionNotification() {
        // No position changes for elem ahead
        
        testee.addFirst(KEY, elem1);
        testee.addLast(KEY, elem2);
        
        sleep();
        
        assertEquals(0, elem1.countReports());
        assertEquals(0, elem2.countReports());
    }
    
    @Test
    public void test_elemInsertedFirst_yieldPositionNotification() {
        // Position changes for elem behind
        
        testee.addFirst(KEY, elem1);
        testee.addFirst(KEY, elem2);
        
        sleep();
        
        assertEquals(0, elem2.countReports());
        
        // First reported position is 2:
        assertEquals(1, elem1.countReports());
        assertEquals(2, elem1.positions().findFirst().getAsLong());
    }
    
    @Test
    public void test_removeElemBehind_noPositionNotification() throws InterruptedException, ExecutionException {
        // No position changes for elem ahead
        
        testee.addFirst(KEY, elem1);
        testee.addLast(KEY, elem2);
        
        assertEquals(true, testee.removeFirstOccurrence(KEY, elem2).get());
        
        sleep();
        
        assertEquals(0, elem1.countReports());
        assertEquals(0, elem2.countReports());
    }
    
    @Test
    public void test_actorAheadDrop() {
        // Position changes for actor behind
        
        testee.addFirst(KEY, elem1);
        testee.addLast(KEY, elem2);
        
        assertEquals(elem1, testee.removeFirst(KEY).get());
        
        sleep();
        
        assertEquals(0, elem1.countReports());
        
        // Moves from initial position 2 to new position 1:
        assertEquals(1, elem2.countReports());
        assertEquals(1, elem2.positions().findFirst().getAsLong());

    }
    
    
    
    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(SLEEP_MS);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}