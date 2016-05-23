package martinandersson.com.lib.concurrent;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;
import static java.util.Objects.requireNonNull;

/**
 * This is a size-aware, concurrent and lock free {@code Deque} manager that
 * provide a key-based abstraction/view into many deques.<p>
 * 
 * The deques are mapped with a key known by the client.<p>
 * 
 * The amount of deques grow and shrink dynamically on demand.<p>
 * 
 * A deque is added<sup>1</sup> when an element is added and there are no
 * previous mapping of the specified key to a deque. A deque is removed when
 * the last element of a deque is removed.<p>
 * 
 * The growing and shrinking are completely transparent to the client. All he
 * have to care about is the key and possibly an element that he want to put in
 * a deque.<p>
 * 
 * This class is basically an abstraction of a {@code Map<K, Deque<E>>} with
 * thread-safe and lock-free access to both the map as well as the deques.<p>
 * 
 * Example:
 * <pre>{@code
 *      ConcurrentDequeManager<String, CustomerComplaint> complaints
 *              = new ConcurrentDequeManager();
 * 
 *      // initialPosition - 1 = number of complaints before this one was added
 *      long initialPosition = complaints.addLast("Product #1234", new CustomerComplaint());
 *      
 *      Optional<CustomerComplaint> complaint = complaints.removeFirstIf(
 *              "Product #1234", CustomerComplaint::hasSuperGrumpyCustomer);
 * }</pre>
 * 
 * Currently, there is no eviction policy implemented nor can one be specified.
 * What goes into a deque stays there (strongly referenced) until explicitly
 * removed by the client.<p>
 * 
 * Getting elements out from the deques managed by this manager can only be done
 * through removing elements. Any number of concurrent invocations to any remove
 * operation provided by this class is all guaranteed to see the return of
 * separate elements (provided there are enough elements to be removed of
 * course).<p>
 * 
 * 
 * 
 * <h2>Position-aware elements</h2>
 * 
 * The manager can notify each element of the element's position in the deque as
 * it changes if the element implements {@linkplain PositionAware}
 * <strong>and</strong> a {@linkplain ExecutorService} was provided to one of
 * the constructors.<p>
 * 
 * It is safe to mix {@code PositionAware} elements with "regular" elements that
 * do not implement the interface.<p>
 * 
 * 
 * 
 * <h2>Weakly consistent</h2>
 * 
 * The information received from {@linkplain #sizeOf(Object)} and position
 * changes that is reported to position-aware elements is only <i>weakly
 * consistent</i>. In English, that means that the value of a reported size-
 * or position does not have to be timely and up-to-date. Another descriptive
 * terminology would be "best-effort basis". For example, there is no guarantee
 * that a position-aware element is reported to have position {@code 1} even
 * though the element at some point in time have that position.<p>
 * 
 * Therefore, client code must only use this type of information for reporting
 * and generation of statistics, or any other type of application that does not
 * require precise information. Displaying this information for a human user in
 * his GUI window what position he has could be one example of a good use.
 * Making a pacemaker depend on the information would not be equally great.<p>
 * 
 * 
 * 
 * <h2>Cost and distribution of workload</h2>
 * 
 * The cost of querying the size of a queue is not O(1) ("constant" time
 * expressed in Big-O notation), or in English; is not as cheap or predictable
 * as reading a <code>long</code> variable neatly stuffed away somewhere. In
 * fact, a call to {@linkplain #sizeOf(Object)} will probably involve some
 * computation of adding together a minor set of counters into one aggregated
 * sum. But the computation is expected to be fast enough for most applications
 * to not bother. Under the hood, the size of each managed deque is tracked by a
 * {@linkplain LongAdder}.<p>
 * 
 * A not as fast operation is the reporting of position changes to all position-
 * aware elements in a deque that became the target of a structural
 * modification. Therefore, the {@code ConcurrentDequeManager} will run these
 * jobs asynchronously using an {@code ExecutorService} that you must provide to
 * a constructor. If none is provided, you will forfeit this feature all
 * together. We refuse to steal the thread from a client who wanted to perform a
 * simple operation as adding or removing an element.<p>
 * 
 * This behavior has two implications you should be aware of:
 * <ol>
 *   <li>Client code that operate the manager can always expect a really fast implementation.</li>
 *   <li>Client code executed in {@code PositionAware.newPosition()} should not thread off but instead block.</li>
 * </ol>
 * 
 * Why number 2? The thread that execute the client's callback come straight
 * from a thread pool. In worst case scenario, if the client's code does not
 * block but instead create new asynchronous tasks on each callback, then this
 * could potentially lead to an {@code OutOfMemoryError} if the system cannot
 * cope with the amount of new task creation. At best, a low-level {@code
 * RejectedExecutionException} would be thrown.<p>
 * 
 * If the work that needs to be done in the "new position" callback is
 * substantial, then use a concurrent data structure on the client-side who
 * receive the positions and then feel free to add asynchronous consumers
 * however you see fit.<p>
 * 
 * The executor service is not only used to report position status changes. Some
 * maintenance work and the work of removing an element that we don't know the
 * position of, will be executed asynchronously too. Well, only if you provide
 * an executor service that is. Otherwise, these tasks are forced to be executed
 * on the client's thread.<p>
 * 
 * 
 * 
 * <h2>Time accuracy</h2>
 * 
 * It's worth writing a note about one more detail. The position of elements and
 * the size of each deque is by no means guaranteed to see a continuous flow of
 * sequential information. The size of the queue will not steadily go from 1 to
 * 5 million. Nor will the last element's reported position be reported 5
 * million times down to 0. This is a design aware trade off chosen in order to
 * prioritize timely correct information more than having non-jagged reports
 * dragging behind schedule, hammering the CPU.<p>
 * 
 * 
 * 
 * <h2>Notes</h2>
 * 
 * 1: The implementation used is {@link ConcurrentLinkedDeque}.<p>
 * 
 * 
 * 
 * @param <K> unique key that map to a deque. Required to override both
 *            {@code Object.hashCode()} and {@code Object.equals(Object)}
 *            properly.
 * @param <E> element of each deque. Should preferably override {@code
 *            Object.equals(Object)} properly.
 * 
 * @author Martin Andersson (webmaster at martinandersson.com)
 */
public final class ConcurrentDequeManager<K, E>
{
    /*
     * A word about my nested type convention.
     * 
     * Nested types can access private symbols in an enclosing class and the
     * other way around. My personal convention, however, is to pretend this
     * "flaw" in Java doesn't exist. I let all private symbols in the enclosing
     * class be accessible/used by nested types, but I keep private symbols
     * inside a nested type as off-market for the enclosing class. The enclosing
     * class must use a non-private API when accessing nested types. If
     * direct-field access is granted, I let these fields have no access
     * modifier at all and no accessors.
     */
    
    
    
    
    /**
     * Used as a callback mechanism by {@linkplain ConcurrentDequeManager} to
     * provide the elements with position notifications.<p>
     * 
     * Please note that the first position is 1 and not 0. Just like in real
     * life that is. Likewise, the last position is the size of the deque.<p>
     * 
     * It is assumed that position-aware objects live in a highly concurrent
     * environment and therefore, the position reported is only weakly
     * consistent.<p>
     * 
     * A position-aware object is not notified when he enter or when he leave a
     * deque. The only thing that is reported is when his already established
     * position in a deque <i>changes</i>.
     * 
     * @author Martin Andersson (webmaster at martinandersson.com)
     */
    @FunctionalInterface
    public interface PositionAware
    {
        /**
         * Called when the element has moved in the deque to a new position.<p>
         * 
         * Currently, all {@code RuntimeException}s thrown by this method is
         * logged, but otherwise consumed by the deque manager. Clients that
         * need exception handling must provide this in the callback.
         * 
         * @param position  the new position, is never {@code < 1}
         * 
         * @see PositionAware
         * @see ConcurrentDequeManager
         */
        void newPosition(long position);
    }
    
    
    
    /*
     *  --------
     * | FIELDS |
     *  --------
     */
    
    private static final Logger LOGGER = Logger.getLogger(ConcurrentDequeManager.class.getName());
    
    /** Does not allow {@code null} to be used as key or value. */
    private final Map<K, DequeWrapper<E>> deques;
    
    /** Might be {@code null} if not specified by client. */
    private final ExecutorService workers;
    
    /** Might be {@code null} if not specified by client. */
    private final ExecutorService administrator;
    
    
    
    /*
     *  --------------
     * | CONSTRUCTORS |
     *  --------------
     */
    
    /**
     * Constructs a deque manager that does not care about position-aware
     * elements.<p>
     * 
     * Zero, some, many or all of the elements feed to this manager may
     * implement {@linkplain PositionAware}. But these elements will not have
     * their position changes reported.
     */
    public ConcurrentDequeManager() {
        workers = administrator = null;
        deques = new ConcurrentHashMap<>();
    }
    
    /**
     * Constructs a deque manager that expect to handle position-aware
     * elements.<p>
     * 
     * Zero, some, many or all of the elements feed to this manager may
     * implement {@linkplain PositionAware}. If zero, then you should use the
     * no-arg constructor.<p>
     * 
     * The administrator thread that schedule the worker threads come
     * from a single thread pool acquired like so:
     * <pre>{@code
     *     Executors.newSingleThreadExecutor();
     * }</pre>
     * 
     * ..which means that the administrator thread is non-daemon and you must
     * not forget to call {@linkplain #close()} before application exit.
     * 
     * @param workers  used to execute jobs that report new positions to
     *                 position-aware elements
     */
    public ConcurrentDequeManager(ExecutorService workers) {
        this.workers = requireNonNull(workers, "workers is null");
        this.administrator = Executors.newSingleThreadExecutor();
        deques = new ConcurrentHashMap<>();
    }
    
    /**
     * Constructs a deque manager that expect to handle position-aware
     * elements.<p>
     * 
     * Zero, some, many or all of the elements feed to this manager may
     * implement {@linkplain PositionAware}. If zero, then you should use the
     * no-arg constructor.<p>
     * 
     * This is the preferred constructor to use for Java SE applications that
     * only want to use daemon threads or for Java EE 7 applications that only
     * use managed resources ({@code ManagedExecutorService}, {@code
     * ManagedThreadFactory}).
     * 
     * @param workers
     *            used to execute jobs that report new positions to
     *            position-aware elements
     * 
     * @param adminThreadFactory
     *            used to create the administrator thread
     */
    public ConcurrentDequeManager(ExecutorService workers, ThreadFactory adminThreadFactory) {
        this.workers = requireNonNull(workers, "workers is null");
        
        requireNonNull(adminThreadFactory, "adminThreadFactory is null");
        this.administrator = Executors.newSingleThreadExecutor(adminThreadFactory);
        
        deques = new ConcurrentHashMap<>();
    }
    
    
    
    // http://stackoverflow.com/a/7097158/1268003
    // http://stackoverflow.com/a/3120727/1268003
    
    
    /*
     *  --------------
     * | EXTERNAL API |
     *  --------------
     */
    
    /**
     * Will put the specified {@code element} in the first position of a deque
     * associated with the specified {@code dequeKey}.<p>
     * 
     * This call might create a new deque if there currently is no deque mapped
     * to the provided key.<p>
     * 
     * The initial position is not reported to position-aware elements according
     * to the JavaDoc of {@linkplain PositionAware}. However, calling this
     * method might be a structural modification of an already existing deque
     * in which case other position-aware elements will receive a new position
     * notification.
     * 
     * @param dequeKey  the deque key
     * @param element   the deque element
     * 
     * @throws NullPointerException if either argument is {@code null}
     */
    public void addFirst(K dequeKey, E element) {
        trace("addFirst", dequeKey, element);
        DequeWrapper<E> q = doAddFirst(dequeKey, element);
        incrementAndReport(q);
    }
    
    /**
     * Will put the specified {@code element} in the last position of a deque
     * associated with the specified {@code dequeKey}.<p>
     * 
     * This call might create a new deque if there currently is no deque mapped
     * to the provided key.<p>
     * 
     * The initial position (returned) is not reported to position-aware
     * elements according to the JavaDoc of {@linkplain PositionAware}.
     * 
     * @param dequeKey  the deque key
     * @param element   the deque element
     * 
     * @return the initial position of the added element (deque size after add)
     * 
     * @throws NullPointerException if either argument is {@code null}
     */
    public long addLast(K dequeKey, E element) {
        trace("addLast", dequeKey, element);
        
        DequeWrapper<E> dw = getOrCreateDeque(dequeKey);
        
        final long initialPosition = dw.getSize() + 1;
        
        ElementWrapper<E> v = new ElementWrapper<>(element, initialPosition);
        
        final boolean added = dw.getDeque().add(v);
        assert added : "ConcurrentLinkedDeque API is totally wrong. Received false!";
        
        dw.sizeIncrement();
        
        return initialPosition;
    }
    
    /**
     * Will remove the first occurrence of the specified {@code element} from a
     * deque associated with the specified {@code dequeKey}.<p>
     * 
     * This is potentially a long-running task and it will be executed
     * asynchronously if this manager was provided an executor service during
     * construction. If not, then the calling thread has to do the work and this
     * call will block until task is completed.
     * 
     * @param dequeKey  the deque key
     * @param element   the deque element
     * 
     * @throws NullPointerException if either argument is {@code null}
     * 
     * @return a {@code Future<Boolean>} whose result says whether an element
     *         was removed ({@code true}) or wasn't found ({@code false})
     */
    public Future<Boolean> removeFirstOccurrence(K dequeKey, E element) {
        trace("removeFirstOccurance", dequeKey, element);
        
        requireNonNull(element, "element is null");
        
        Optional<DequeWrapper<E>> opt = this.getDoNotCreateDeque(dequeKey);
        
        if (!opt.isPresent()) {
            return CompletedFuture(false, null);
        }
        
        DequeWrapper<E> dw = opt.get();
        
        return tryRunAsync(() -> {
            /*
             * Can not use Deque.remove(Object)/removeFirstOccurance(Object)
             * (same thing) since these methods do not give us a reference to
             * the element removed. We need the actual element removed so we can
             * set his removed-flag. But I expect no significant difference in
             * time cost using Iterator versus Deque.remove(Object).
             */
            
            Iterator<ElementWrapper<E>> it = dw.getDeque().iterator();
            
            while (it.hasNext()) {
                ElementWrapper<E> ew = it.next();
                
                if (ew.getValue().equals(element) && ew.markRemoved()) {
                    it.remove();
                    decrementAndReport(dequeKey, dw);
                    return true;
                }
            }
            
            return false;
        });
    }
    
    /**
     * Will remove and return the first element (head) from a deque associated
     * with the specified {@code dequeKey}.<p>
     * 
     * The returned optional will be empty only if there was no deque mapped to
     * the key.<p>
     * 
     * @param dequeKey  the deque key
     * 
     * @return the first [and removed] element discovered in the deque
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     */
    public Optional<E> removeFirst(K dequeKey) {
        trace("removeFirst", dequeKey);
        return doRemoveFirstAndDecrement(dequeKey, null);
    }
    
    /**
     * Will remove and return the first element (head) from a deque associated
     * with the specified {@code dequeKey} - but only if first element is
     * approved by the specified {@code predicate}.<p>
     * 
     * The returned optional will be empty only if there was no deque mapped to
     * the key or predicate returned {@code false}.<p>
     * 
     * Please note that the specified {@code predicate} may in exceptional
     * circumstances be called more than one time. The only thing guaranteed -
     * if the deque contain unique elements - is that the predicate will be feed
     * a new element each time. Multiple invocations will only happen if a
     * concurrent remove operation succeeds in removing an element after it is
     * peeked and tested by this method but before this method complete his
     * remove operation.
     * 
     * @param dequeKey   the deque key
     * @param predicate  a predicate
     * 
     * @return the first [and removed] element discovered in the deque
     * 
     * @throws NullPointerException if any argument is {@code null}
     */
    public Optional<E> removeFirstIf(K dequeKey, Predicate<? super E> predicate) {
        trace("removeFirstIf", dequeKey, predicate);
        return doRemoveFirstAndDecrement(dequeKey, requireNonNull(predicate));
    }
    
    /**
     * Returns {@code true} if the deque manager has a deque mapped to the
     * provided key, otherwise {@code false}.
     * 
     * @param dequeKey the key
     * 
     * @return {@code true} if the deque manager has a deque mapped to the
     *         provided key, otherwise {@code false}
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     */
    public boolean hasDeque(K dequeKey) {
        return this.getDoNotCreateDeque(dequeKey).isPresent();
    }
    
    /**
     * Will query the most up-to-date size of a deque (weakly consistent).<p>
     * 
     * If the key does not map to a deque, then {@code 0} is returned.<p>
     * 
     * @param dequeKey  the key
     * 
     * @return the size of the deque
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     */
    public long sizeOf(K dequeKey) {
        return getDoNotCreateDeque(dequeKey)
                .map(DequeWrapper::getSize)
                .orElse(0L);
    }
    
    /**
     * Will close the underlying thread pools, if they exist.<p>
     * 
     * If they do not exist, then this method is NOOP.<p>
     * 
     * This method do not wait for currently executing jobs of reporting
     * element positions to terminate.
     */
    public void close() {
        if (administrator != null) {
            administrator.shutdownNow();
        }
        
        if (workers != null) {
            workers.shutdownNow();
        }
    }
    
    /**
     * Low-level method for debugging only.
     * 
     * @param dequeKey  the key
     * @param action    the action to apply on all elements
     * 
     * @throws NullPointerException if no deque was mapped to the key
     */
    void forEach(K dequeKey, Consumer<? super E> action) {
        deques.get(dequeKey).getDeque().stream()
                .map(ElementWrapper::getValue)
                .forEach(action);
    }
    
    
    
    /*
     *  --------------
     * | INTERNAL API |
     *  --------------
     */
    
    /**
     * Log method entry with parameters on {@code Level.FINER}.
     */
    private void trace(String method, Object... args) {
        LOGGER.entering(ConcurrentDequeManager.class.getSimpleName(), method, args);
    }
    
    /**
     * Remove head from deque.<p>
     * 
     * If head was removed, then remaining position-aware elements will have
     * their new positions reported.
     * 
     * @param dequeKey   deque key
     * @param predicate  optional predicate
     * 
     * @return the head
     */
    private Optional<E> doRemoveFirstAndDecrement(K dequeKey, Predicate<? super E> predicate) {
        Optional<Entry<E, E>> head = doRemoveFirst(dequeKey, predicate);
        
        if (!head.isPresent()) {
            return Optional.empty();
        }
        
        // Will trigger report of new positions:
        decrementAndReport(dequeKey, head.get().dequeWrapper);
        
        return Optional.of(head.get().elementWrapper.getValue());
    }
    
    /**
     * Will remove and return the head of the mapped deque.<p>
     * 
     * A {@code predicate} may optionally be provided.<p>
     * 
     * If the deque became empty, it will be removed. No other side effects
     * happen: position changes are not reported.
     * 
     * @param dequeKey   the key
     * @param predicate  the predicate (optional)
     * 
     * @return the {@code DequeWrapper<E>} and {@code ElementWrapper<E>} put
     *         together into an {@code Entry<E, E>}
     * 
     * @throws NullPointerException  if {@code dequeKey} is {@code null}
     */
    private Optional<Entry<E, E>> doRemoveFirst(K dequeKey, Predicate<? super E> predicate) {
        Optional<DequeWrapper<E>> opt = getDoNotCreateDeque(dequeKey);
        
        // We found no deque?
        if (!opt.isPresent()) {
            LOGGER.finer(() ->
                    "Asked to remove first of key " + dequeKey  + "; but I found no such deque in store.");
            
            return Optional.empty();
        }
        
        final DequeWrapper<E> dw = opt.get();
        
        final Iterator<ElementWrapper<E>> it = dw.getDeque().iterator();
        
        if (!it.hasNext()) {
            LOGGER.finer(() ->
                    "Asked to remove first of key " + dequeKey  + "; but the deque was empty.");

            removeEmptyQueue(dequeKey, dw);
            return Optional.empty();
        }
        
        while (it.hasNext()) {
            final ElementWrapper<E> head = it.next();
            
            if (predicate != null && !predicate.test(head.getValue())) {
                break;
            }
            
            if (head.markRemoved()) {
                it.remove();
                
                if (dw.getDeque().isEmpty()) {
                    removeEmptyQueue(dequeKey, dw);
                }
                
                return Optional.of(new Entry<>(dw, head));
            }
        }
        
        return Optional.empty();
    }
    
    /**
     * Will insert the element at the front of the mapped deque.<p>
     * 
     * If the deque does not exist, it will be created. There are no other
     * side effects: position changes are not reported.
     * 
     * @param dequeKey  the key that maps (or will be mapped) to a deque
     * @param element   the element to be added
     * 
     * @return the deque that the element was added to
     */
    private DequeWrapper<E> doAddFirst(K dequeKey, E element) {
        DequeWrapper<E> dw = getOrCreateDeque(dequeKey);
        
        ElementWrapper<E> v = new ElementWrapper<>(element, 1);
        dw.getDeque().addFirst(v);
        
        return dw;
    }
    
    /**
     * Will increment the specified {@code deque} by {@code 1} and report
     * possible position changes to position-aware elements.
     * 
     * @param deque  the deque
     */
    private void incrementAndReport(DequeWrapper<E> deque) {
        deque.sizeIncrement();
        
        deque.setReportingNeeded();
        reportPositionChanges();
    }
    
    /**
     * Will decrement the specified {@code deque} by {@code 1}, asynchronously
     * remove an empty deque and report possible position changes to
     * position-aware elements.
     * 
     * @param dequeKey  deque key
     * @param expected  expected deque to be found using the key
     */
    private void decrementAndReport(K dequeKey, DequeWrapper<E> expected) {
        expected.sizeDecrement();
        
        // Deque MIGHT have become empty:
        removeQueueIfEmpty(dequeKey, expected);
        
        expected.setReportingNeeded();
        reportPositionChanges();
    }
    
    /**
     * Will get an already existing deque, or create one if there was no such
     * deque mapped to the specified {@code key}.
     * 
     * @param dequeKey  the deque key
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     * 
     * @returns always a non-null {@code DequeWrapper<E>}
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     */
    private DequeWrapper<E> getOrCreateDeque(K dequeKey) {
        requireNonNull(dequeKey, "dequeKey is null");
        return deques.computeIfAbsent(dequeKey, ignoredKey -> new DequeWrapper<>());
    }
    
    /**
     * Returns a deque mapped to the specified {@code dequeKey}.<p>
     * 
     * This method has no side effects such as creating a deque.
     * 
     * @param dequeKey  deque key
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     * 
     * @returns a deque mapped to the specified {@code dequeKey}
     */
    private Optional<DequeWrapper<E>> getDoNotCreateDeque(K dequeKey) {
        requireNonNull(dequeKey, "dequeKey is null");
        return Optional.ofNullable(deques.get(dequeKey));
    }
    
    /**
     * Will remove the assumed to be empty deque, if the manager currently store
     * the {@code expected} deque.<p>
     * 
     * The size of the deque is not evaluated. But as a necessary safe guard,
     * elements left behind in the removed deque will we enqueued using
     * {@linkplain #addFirst(K, E)}. This should in practice happen very rarely.
     * 
     * @param dequeKey  deque key
     * @param expected  deque expected to be mapped to the provided key
     * 
     * @return {@code true} if we found the deque and removed it, otherwise
     *         {@code false}
     * 
     * @throws NullPointerException if any of the arguments are {@code null}
     */
    private boolean removeEmptyQueue(K dequeKey, DequeWrapper<E> expected) {
        // Possible NPE:
        final boolean success = deques.remove(dequeKey, expected);
        
        if (success) {
            LOGGER.finer(() ->
                    "Successfully removed empty deque mapped to: " + dequeKey + ".");
        }
        else {
            LOGGER.finer(() ->
                    "Tried to but did not succeed (deque already removed or new deque already assigned) in removal of empty deque mapped to: " + dequeKey + ".");
        }
        
        expected.sizeReset();
        
        /*
         * What harm would the following line of code do if the queue really is empty ;)
         * BUT it could be in the concurrent world we live in that the queue actually has some elements.
         */
        expected.getDeque().forEach(e ->
                addFirst(dequeKey, e.getValue()));
        
        return success;
    }
    
    /**
     * Will inspect the actual known size of the queue and remove him only if he
     * was found to be empty.<p>
     * 
     * This method will try to run his job asynchronously, if an {@code
     * ExecutorService} was provided during construction of the manager.
     * 
     * @param queueKey  the key
     * @param expected  expected deque
     * 
     * @throws NullPointerException if any argument is {@code null}
     */
    private void removeQueueIfEmpty(K queueKey, DequeWrapper<E> expected) {
        tryRunAsync(() -> {
            if (sizeOf(queueKey) <= 0L) {
                removeEmptyQueue(queueKey, expected);
            }
        });
    }
    
    private void tryRunAsync(Runnable job) {
        tryRunAsync(Executors.callable(job));
    }
    
    private <T> Future<T> tryRunAsync(Callable<T> job) {
        if (workers != null) {
            try {
                return workers.submit(job);
            }
            catch (RejectedExecutionException e) {
                LOGGER.warning(() ->
                        "Tried running job asynchronously but got RejectedExecutionException. Running job in calling thread instead.");
                
                return CompletedFuture(job);
            }
        }
        else {
            return CompletedFuture(job);          
        }
    }
    
    private final Lock reporting = new ReentrantLock();
    
    private volatile boolean reportWorkScheduled = false;
    
    /**
     * If an executor service was provided, all position-aware elements in
     * marked deques will have their new position reported (given that the
     * position actually changed).<p>
     * 
     * Safe to invoke from client's thread regardless of whether an executor
     * service was provided or not. If client never provided an executor
     * service, then this method is NOOP.<p>
     * 
     * It is safe to invoke this method many times over, concurrently or
     * otherwise. At most only "one job" of reporting element positions will
     * execute. If such a job is already running, another one will be scheduled
     * to run in sequence as soon as the ongoing job is completed. At most one
     * such job will be scheduled.
     */
    private void reportPositionChanges() {
        trace("reportPositionChanges");
        
        if (workers == null) {
            return;
        }
        
        // Schedule job to report:
        reportWorkScheduled = true;
        
        Runnable adminJob = new Runnable() {
            @Override public void run() {
                if (!reporting.tryLock()) {
                    // It's okay, we just set the secret flag a moment ago.
                    // Whover is running a reporting job will rerun the job when current job completes.
                    return;
                }
                
                try {
                    while (reportWorkScheduled || needToReportPositions()) thenReport: {
                        reportWorkScheduled = false;
                        
                        // One task per [dirty] Deque:
                        List<Callable<Object>> callables = dequesThatNeedReporting()
                                .map((d) -> Executors.callable(() -> doReport(d)))
                                .collect(toList());

                        // Admin's task: submit to executor and wait for all jobs to finish:
                        try {
                            workers.invokeAll(callables);
                        }
                        catch (InterruptedException e) {
                            LOGGER.log(Level.WARNING, "Interrupted while waiting for reporting jobs to finish.", e);
                        }
                    }
                }
                finally {
                    reporting.unlock();
                }

                // If someone scheduled a job after our lock release (not probable but possible), then call self:
                if (reportWorkScheduled) {
                    this.run();
                }
            }
        };
        
        try {
            administrator.execute(adminJob);
        }
        catch (RejectedExecutionException e) {
            LOGGER.warning(() ->
                    "Tried to report element positions using the administrator thread but got RejectedExecutionException. Running job in calling thread instead.");
            
            adminJob.run();
        }
    }
    
    /**
     * Returns {@code true} if at least one deque has been marked to need
     * position reports, otherwise {@code false}.
     * 
     * @return {@code true} if at least one deque has been marked to need
     *         position reports, otherwise {@code false}
     */
    private boolean needToReportPositions() {
        return deques.values().stream().anyMatch(DequeWrapper::shouldReport);
    }
    
    /**
     * @return all deques that has been marked to need position reports
     */
    private Stream<DequeWrapper<E>> dequesThatNeedReporting() {
        return deques.values().stream().filter(DequeWrapper::shouldReport);
    }
    
    /**
     * Will report a new position to all the position-aware elements found in
     * the specified {@code deque}.
     */
    private void doReport(DequeWrapper<E> deque) {
        if (!deque.shouldReport()) {
            return;
        }
        
        deque.setReportingDone();
        long actualPos = 0L;
        
        for (ElementWrapper<E> ew : deque.getDeque()) {
            ++actualPos;
            
            final E val = ew.getValue();
            
            if (val instanceof PositionAware && ew.getLastKnownPosition() != actualPos) {
                ew.setLastKnownPosition(actualPos);
                
                try {
                    ((PositionAware) val).newPosition(actualPos);
                }
                catch (RuntimeException e) {
                    LOGGER.log(Level.WARNING, "Caught an exception while trying to report a new position!", e);
                }
            }
        }
    }
    
    
    
    /*
     *  ----------------
     * | INTERNAL TYPES |
     *  ----------------
     */
    
    /**
     * Builds an already completed future with a result provided by the
     * specified {@code supplier}, or with an exception thrown by the supplier.
     * 
     * @param <T>        The result type returned by the Future's get method
     * 
     * @return an already completed future with a result provided by the
     * specified {@code supplier}, or with an exception thrown by the supplier
     */
    private static <T> Future<T> CompletedFuture(Callable<T> supplier) {
        try {
            return CompletedFuture(supplier.call(), null);
        }
        catch (Exception e) {
            return CompletedFuture(null, e);
        }
    }
    
    /**
     * Builds an already completed future with result or exception provided as
     * arguments.
     * 
     * @param <T>        The result type returned by the Future's get method
     * @param result     result
     * @param exception  exception
     * 
     * @return an already completed future with result or exception provided as
     * arguments
     */
    private static <T> Future<T> CompletedFuture(T result, Exception exception) {
        return new Future<T>() {
            @Override public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }
            
            @Override public boolean isCancelled() {
                return false;
            }
            
            @Override public boolean isDone() {
                return true;
            }
            
            @Override public T get() throws ExecutionException {
                if (exception != null) {
                    throw new ExecutionException(exception);
                }
                
                return result;
            }
            
            @Override public T get(long timeout, TimeUnit unit) throws ExecutionException {
                return get();
            }
        };
    }
    
    private static class DequeWrapper<E>
    {
        private final ConcurrentLinkedDeque<ElementWrapper<E>> deque = new ConcurrentLinkedDeque<>();
        
        private final LongAdder size = new LongAdder();
        
        /**
         * Flag to mark that a deque has undergone a structural modification
         * and this deque's elements need a new position notification.
         */
        private volatile boolean report = false;
        
        
        
        public ConcurrentLinkedDeque<ElementWrapper<E>> getDeque() {
            return deque;
        }
        
        /**
         * Returns {@code true} if this deque has undergone a structural change
         * and it's position-aware elements need a position notification,
         * otherwise {@code false}.
         * 
         * @return {@code true} if this deque has undergone a structural change
         * and it's position-aware elements need a position notification,
         * otherwise {@code false}
         */
        public boolean shouldReport() {
            return report;
        }
        
        /**
         * Set/mark this deque as having undergone a structural change:
         * position-aware elements need a position notification.
         */
        public void setReportingNeeded() {
            report = true;
        }
        
        /**
         * Set/mark this deque as having had a job perform a fresh distribution
         * of position notifications to all its position-aware elements.
         */
        public void setReportingDone() {
            report = false;
        }
        
        public long getSize() {
            return size.sum();
        }
        
        public void sizeIncrement() {
            size.increment();
        }
        
        public void sizeDecrement() {
            size.decrement();
        }
        
        public void sizeReset() {
            size.reset();
        }
        
        // Default impl. of equals and hashCode using == is just what we want!
    }
    
    private static class ElementWrapper<E>
    {
        private final E value;
        
        private final AtomicBoolean removed = new AtomicBoolean();
        
        private volatile long lastKnownPosition;
        
        
        public ElementWrapper(E value, long lastKnownPosition) {
            this.value = requireNonNull(value, "element is null");
            this.lastKnownPosition = lastKnownPosition;
        }
        
        
        public E getValue() {
            return value;
        }
        
        /**
         * Will mark this element to be removed from its deque.<p>
         * 
         * The thread that mark this element for removal is the guy who also has
         * to remove the element as soon as possible. Preferably, the remove
         * call should be the very next statement executed. Once an element has
         * been "marked removed", then no other thread will ever again attempt
         * removing the element. So we must not risk crashing inbetween the mark
         * and actual removal.<p>
         * 
         * Please note that the manager export a few different ways for an
         * element to be removed from his deque:<p>
         * 
         * <ol>
         *   <li>Remove head.</li>
         *   <li>Remove first occurence of specified element.</li>
         *   <li>Remove first occurence of specified element - if predicate match.</li>
         * </ol>
         * 
         * I can not explain, nor do I really care, exactly how all remove
         * methods of {@code ConcurrentLinkedDeque} work. But I expect that all
         * weakly consistent iterators run through a snapshot and it is possible
         * that {@code next()} actually return an element that was just
         * removed.<p>
         * 
         * There's no fix for that problem as long as we want all the benefits
         * of concurrent collections. Weakly consistent iterators must spit out
         * an element if a previous call to {@code hasNext()} returned {@code
         * true}. Not much they can do if the element was removed inbetween.
         * Similarly, this problem is particularly evident if we ever decide to
         * use a remove-implementation that call {@code Deque.peekFirst()} only
         * to run some tests on the element and 5 minutes later call {@code
         * Deque.removeFirst()}.<p>
         * 
         * But, we on the client-side may (and do) implement a trick. The thread
         * that successfully mark this element for removal will also see a
         * return value of {@code true} and this thread will now have the
         * permit/obligation to actually remove the element. All other threads
         * must continue their iteration and discard the knowledge of the
         * "already removed" element as if they never saw it.<p>
         * 
         * With this flag in place, two or five billion different concurrent
         * remove-method invocations by the manager's clients will see the exact
         * same number of elements removed. This greatly simplifies client code
         * who can expect to retrieve unique elements and treat them
         * accordingly. From the client's perspective, there's no "concurrent"
         * semantics in the remove operations anymore at the same time he can
         * expect to see about the same performance.
         */
        public boolean markRemoved() {
            return removed.compareAndSet(false, true);
        }

        public long getLastKnownPosition() {
            return lastKnownPosition;
        }

        public void setLastKnownPosition(long lastKnownPosition) {
            this.lastKnownPosition = lastKnownPosition;
        }
        
        
        @Override
        public int hashCode() {
            return value.hashCode(); }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            
            if (obj == null) {
                return false;
            }
            
            if (ElementWrapper.class != obj.getClass()) {
                return false;
            }
            
            return this.value.equals(((ElementWrapper) obj).value);
        }
    }
    
    private static class Entry<V, E>
    {
        final DequeWrapper<V> dequeWrapper;
        
        final ElementWrapper<E> elementWrapper;
        
        Entry(DequeWrapper<V> queueWrapper, ElementWrapper<E> elementWrapper) {
            this.dequeWrapper = queueWrapper; this.elementWrapper = elementWrapper;
        }
    }
}