package martinandersson.com.lib.concurrent;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Will test basic functionality of a single-threaded {@code
 * ConcurrentDequeManager}.
 * 
 * @author Martin Andersson (webmaster at martinandersson.com)
 */
public class SimpleTest
{
    private final static String KEY1   = "deque_key_1",
                                KEY2   = "deque_key_2",
                                VALUE1 = "some_value_1",
                                VALUE2 = "some_value_2";
    
    ConcurrentDequeManager<String, String> testee;
    
    
    
    /*
     *  ------------
     * | LIFE CYCLE |
     *  ------------
     */
    
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
        testee = new ConcurrentDequeManager<>();
    }

    
    
    /*
     *  -------
     * | TESTS |
     *  -------
     */
    
    @Test
    public void addFirst()
    {
        testee.addFirst(KEY1, VALUE1);
        
        assertElement(VALUE1, testee.removeFirst(KEY1));
        assertEmptyDequeManager(KEY1);
    }
    
    @Test
    public void addLast()
    {
        testee.addLast(KEY1, VALUE1);
        
        assertElement(VALUE1, testee.removeFirst(KEY1));
        assertEmptyDequeManager(KEY1);
    }
    
    @Test
    public void removeFirstOccurance() throws InterruptedException, ExecutionException
    {
        testee.addFirst(KEY1, VALUE1);
        
        // Salt:
        testee.addLast(KEY1, VALUE2);
        testee.addFirst(KEY2, VALUE1);
        
        assertEquals(true, testee.removeFirstOccurance(KEY1, VALUE2).get());
        
        assertNonEmptyDequeManager(KEY1, 1);
        assertNonEmptyDequeManager(KEY2, 1);
        
        assertElement(VALUE1, testee.removeFirst(KEY1));
    }
    
    
    @Test
    public void removeFirstIf()
    {
        testee.addFirst(KEY1, VALUE1);
        
        // Salt:
        testee.addLast(KEY1, VALUE2);
        testee.addFirst(KEY2, VALUE1);
        
        assertEquals(false, testee.removeFirstIf(KEY1, Predicate.isEqual("no match please")).isPresent());
        assertElement(VALUE1, testee.removeFirstIf(KEY1, Predicate.isEqual(VALUE1)));
        
        assertNonEmptyDequeManager(KEY1, 1);
        assertNonEmptyDequeManager(KEY2, 1);
        
        assertElement(VALUE2, testee.removeFirst(KEY1));
    }
    
    
    
    /*
     *  --------
     * | ASSERT |
     *  --------
     */
    
    private void assertElement(String expected, Optional<String> actual) {
        assertEquals(true, actual.isPresent());
        assertEquals(expected, actual.get());
    }
    
    private void assertEmptyDequeManager(String key) {
        assertEquals(false, testee.hasDeque(key));
        assertEquals(0, testee.sizeOf(key));
    }
    
    private void assertNonEmptyDequeManager(String key, int size) {
        assertEquals(true, testee.hasDeque(key));
        assertEquals(size, testee.sizeOf(key));
    }
}