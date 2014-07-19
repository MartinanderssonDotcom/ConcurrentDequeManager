package martinandersson.com.lib.concurrent;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <h2>The manager</h2>
 * 
 * This is a size-aware, highly concurrent and lock free deque manager that
 * provides a key-based abstraction/view into many deques.<p>
 * 
 * The deques are mapped with a key known by the client.<p>
 * 
 * The amount of deques grow and shrink dynamically on demand. They grow (a
 * deque is added) whenever an element is added when there was no previous
 * mapping of the key to a deque, and the amount of deques shrinks (a deque is
 * removed) whenever an element of a deque is removed such that the mapped deque
 * becomes empty. This process is completely transparent for the client. All he
 * cares about is the key and the element that the client want to put in a deque
 * or have removed from one.<p>
 * 
 * Currently, there is no eviction policy implemented. What goes into a deque
 * stays there (strongly referenced) until explicitly removed by the client.<p>
 * 
 * Scroll to the bottom of this JavaDoc for some code examples.<p>
 * 
 * 
 * 
 * <h2>Position aware elements</h2>
 * 
 * The manager can notify each element of the element's position in the deque as
 * it changes if the element implements {@linkplain PositionAware}
 * <strong>and</strong> if a {@linkplain ExecutorService} was provided to one of
 * constructors.<p>
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
 * Making a pacemaker depend on the information would not be that great.<p>
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
 * aware elements in a deque that became the target of a structural modification.
 * Therefore, the {@code ConcurrentDequeManager} will run these jobs
 * asynchronously using an {@code ExecutorService} that you must provide to a
 * constructor, or forfeit the feature all together. We simply cannot risk to
 * block the client's execution path while his thread spins of trying to report
 * five billion new positions (all being the result of a simple add or remove!).
 * This behavior has two implications you should be aware of:
 * 
 * <ol>
 *   <li>Client code that operate the manager can do business as usual and always expect really fast implementations.</li>
 *   <li>Client code that is executed as a result of using position-aware elements, should not thread off but instead block.</li>
 * </ol>
 * 
 * Why number 2? The thread that execute the client's callback come straight
 * from a thread pool. In worst case scenario, if the client's code does not
 * block but instead create new asynchronous tasks on each callback, then this
 * could potentially lead to a memory leak. The memory leak will only happen if
 * the system cannot cope with the amount of continuous new task creation. At
 * best, a low-level {@code RejectedExecutionException} will be thrown. The
 * callback is supposed to be a quick job, so let the worker thread complete his
 * job before he moves on to report the position of the next position-aware
 * element.<p>
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
 * <h2>TODO: USE CASES (I guess see the test files for now?)</h2>
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
public class ConcurrentDequeManager<K, E>
{
    /*
     *  ----------------
     * | STATIC HELPERS |
     *  ----------------
     */
    
    protected static final <T> Future<T> CompletedFuture(T result, Optional<Exception> exception) // <-- final disable the possibility of hiding a static method
    {
        return new Future<T>()
        {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false; }

            @Override
            public boolean isCancelled() {
                return false; }

            @Override
            public boolean isDone() {
                return true; }

            @Override
            public T get() throws ExecutionException {
                if (exception.isPresent())
                    throw new ExecutionException(exception.get());
                return result; }

            @Override
            public T get(long timeout, TimeUnit unit) throws ExecutionException {
                return get(); }
        };
    }
    
    
    
    /*
     *  --------
     * | FIELDS |
     *  --------
     */
    
    private final static Logger LOGGER = Logger.getLogger(ConcurrentDequeManager.class.getName());
    
    private final Map<K, DequeWrapper<E>> DEQUES = new ConcurrentHashMap<>();
    
    private final ExecutorService workers, administrator;
    
    
    
    /*
     *  --------------
     * | CONSTRUCTORS |
     *  --------------
     */
    
    /**
     * Constructs a deque manager that does not care about position-aware
     * elements.<p>
     * 
     * It isn't illegal to use position-aware elements with the manager's
     * deques, but these elements will not have their position changes reported.
     */
    public ConcurrentDequeManager() {
        workers = administrator = null;
    }
    
    /**
     * Constructs a deque manager that expect to handle position-aware
     * elements.<p>
     * 
     * Zero, some, many or all of the elements may implement {@linkplain
     * PositionAware}. If zero, then you should use the no-arg constructor
     * instead.<p>
     * 
     * The administrator thread that schedule the worker threads come
     * from a single thread pool acquired like so:<pre>{@code
     * 
     *     Executors.newSingleThreadExecutor();
     * }</pre>
     * 
     * ..which means that the administrator thread is non-daemon and you must
     * not forget to call {@linkplain #close()} before application exit.
     * 
     * @param workers used to execute jobs that report position changes to
     *        position-aware elements
     */
    public ConcurrentDequeManager(ExecutorService workers) {
        this.workers = Objects.requireNonNull(workers, "workers is null");
        this.administrator = Executors.newSingleThreadExecutor();
    }
    
    /**
     * Constructs a deque manager that expect to handle position-aware
     * elements.<p>
     * 
     * Zero, some, many or all of the elements may implement {@linkplain
     * PositionAware}. If zero, then you should use the no-arg constructor
     * instead.<p>
     * 
     * This is the preferred constructor to use for Java SE applications that
     * only want to use daemon threads or for Java EE applications that only
     * use managed resources: {@code ManagedExecutorService} and {@code
     * ManagedThreadFactory}.
     * 
     * @param workers used to execute jobs that report position changes to
     *        position-aware elements
     * @param adminThreadFactory used to create the administrator thread
     * 
     * @throws NullPointerException if any of the arguments are <code>null</code>.
     */
    public ConcurrentDequeManager(ExecutorService workers, ThreadFactory adminThreadFactory)
    {
        this.workers = Objects.requireNonNull(workers, "workers is null");
        
        Objects.requireNonNull(adminThreadFactory, "adminThreadFactory is null");
        this.administrator = Executors.newSingleThreadExecutor(adminThreadFactory);
    }
    

    
    
    // http://stackoverflow.com/a/7097158/1268003
    // http://stackoverflow.com/a/3120727/1268003
    
    
    /*
     *  --------------
     * | EXTERNAL API |
     *  --------------
     */
    
    /**
     * Will put the provided element in the front of the key-associated deque.<p>
     * 
     * This call might create a new deque if there currently is no deque mapped
     * to the provided key. Therefore, this method call always succeed.<p>
     * 
     * The initial position is not reported to position-aware elements according
     * to the JavaDoc contract of {@linkplain PositionAware}. However, this is
     * a structural modification such that other position-aware elements in the
     * deque will be reported a new position.
     * 
     * @param dequeKey the deque key
     * @param element the deque element
     * 
     * @throws NullPointerException if either argument is {@code null}
     */
    public void addFirst(K dequeKey, E element)
    {
        trace("addFirst", dequeKey, element);
        DequeWrapper<E> q = doAddFirst(dequeKey, element);
        incrementAndReport(q);
    }
    
    /**
     * Will put the provided element in the back of the key-associated deque.<p>
     * 
     * This call might create a new deque if there currently is no deque mapped
     * to the provided key. Therefore, this method call always succeed.<p>
     * 
     * @param dequeKey the deque key
     * @param element the deque element
     * 
     * @return The initial position of the added element (deque size after add).
     *         The initial position is not reported to the element according to
     *         the JavaDoc contract of {@linkplain PositionAware}.
     * 
     * @throws NullPointerException if either argument is {@code null}
     */
    public long addLast(K dequeKey, E element)
    {
        trace("addLast", dequeKey, element);
        
        DequeWrapper<E> d = getOrCreateDeque(dequeKey);
        
        final long initialPosition = d.counter.sum() + 1;
        
        ElementWrapper<E> v = new ElementWrapper<>(element, initialPosition);
        
        final boolean isTrue = d.deque.add(v);
        assert isTrue : "ConcurrentLinkedDeque API is totally wrong. Received false!";
        
        d.counter.increment();
        
        return initialPosition;
    }
    
    /**
     * Will remove first occurrence of the provided element from the mapped
     * deque.<p>
     * 
     * This is potentially a long-running task. Therefore, the task will be
     * executed asynchronously if the manager constructor was provided an
     * executor service. If not, then the calling thread has to do the work. In
     * the latter case, the method will block and a subsequent call to
     * {@linkplain Future#get()} will not block.
     * 
     * @param dequeKey the deque key
     * @param element the deque element
     * 
     * @throws NullPointerException if either argument is {@code null}
     * 
     * @return a {@code Future<Boolean>} with or without an immediate result
     *         that says whether an element was removed ({@code true}) or wasn't
     *         found ({@code false})
     */
    public Future<Boolean> removeFirstOccurance(K dequeKey, E element)
    {
        trace("removeFirstOccurance", dequeKey, element);
        
        Objects.requireNonNull(element, "element is null");
        
        Optional<DequeWrapper<E>> d1 = this.getDoNotCreateDeque(dequeKey);
        
        if (!d1.isPresent())
            return CompletedFuture(false, Optional.empty());
        
        ElementWrapper<E> expected = new ElementWrapper<>(element);
        
        DequeWrapper<E> d2 = d1.get();
        
        return tryRunAsync(() ->
        {
            final boolean success = d2.deque.remove(expected);
            
            // TODO: Mark ElementWrapper as removed!?
            
            if (success) {
                decrementAndReport(dequeKey, d2);
            }
            
            return success;
        });
    }
    
    /**
     * Will remove and return the first element of the key-mapped deque.<p>
     * 
     * If there was no deque mapped to the key, an empty {@code Optional<E>} is
     * returned.<p>
     * 
     * @param dequeKey the deque key
     * 
     * @return the first [and removed] element discovered in the deque
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     */
    public Optional<E> removeFirst(K dequeKey)
    {
        trace("removeFirst", dequeKey);
        return doRemoveFirstAndDecrement(dequeKey, Optional.empty());
    }
    
    /**
     * Will remove and return the first element of the key-mapped deque, only if
     * the element match the provided predicate.<p>
     * 
     * If there was no deque mapped to the key or the element did not match the
     * provided predicate, an empty {@code Optional<E>} is returned.<p>
     * 
     * @param dequeKey the deque key
     * @param predicate the predicate to match the element against
     * 
     * @return the first [and removed] element discovered in the deque
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     */
    public Optional<E> removeFirstIf(K dequeKey, Predicate<? super E> predicate)
    {
        trace("removeFirstIf", dequeKey, predicate);
        return doRemoveFirstAndDecrement(dequeKey, Optional.of(predicate));
    }
    
    /**
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
     * Will query the most up-to-date size of a deque (weakly consistent). If
     * the key does not map to a deque, {@code 0} is returned.<p>
     * 
     * @param dequeKey the key
     * 
     * @return the size of the deque
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     */
    public long sizeOf(K dequeKey)
    {
        Optional<DequeWrapper<E>> wrapper = getDoNotCreateDeque(dequeKey);
        
        if (!wrapper.isPresent())
            return 0L;
        
        return sizeOf(wrapper.get());
    }
    
    /**
     * Will close the thread pools, if used.<p>
     * 
     * Does not wait for actively executing tasks to terminate.
     */
    public void close()
    {
        if (administrator != null)
            administrator.shutdownNow();
        
        if (workers != null)
            workers.shutdownNow();
    }
    
    /**
     * Low-level method for debugging only.
     * 
     * @param dequeKey the key
     * @param action the action to apply on all elements
     * 
     * @throws NullPointerException if no deque was mapped to the key
     */
    protected void forEach(K dequeKey, Consumer<? super E> action) {
        DEQUES.get(dequeKey).deque.stream().map(wrapper -> wrapper.value).forEach(action);
    }
    
    
    
    /*
     * ----------------
     * | INTERNAL API |
     * ----------------
     */
    
    /** Log method entry with parameters on {@code Level.FINER}. */
    private void trace(String method, Object... args) {
        LOGGER.entering(ConcurrentDequeManager.class.getSimpleName(), method, args);
    }
    
    /**
     * Will get the most up-to-date size of the provided deque (weakly
     * consistent).
     * 
     * @param deque the deque
     * 
     * @return the size of the deque
     * 
     * @throws NullPointerException if deque is {@code null}
     */
    private long sizeOf(DequeWrapper<E> deque) {
        return deque.counter.sum();
    }
    
    /**
     * Remove head from deque.<p>
     * 
     * If head was removed, then remaining position-aware elements will have
     * their new positions reported.
     * 
     * @param dequeKey the key
     * @param predicate the optional predicate
     * 
     * @return the head
     */
    private Optional<E> doRemoveFirstAndDecrement(K dequeKey, Optional<Predicate<? super E>> predicate)
    {
        Optional<Entry<E, E>> head = predicate.isPresent() ?
                doRemoveFirstIf(dequeKey, predicate.get()) :
                doRemoveFirst(dequeKey);
        
        if (!head.isPresent())
            return Optional.empty();
        
        DequeWrapper<E> d = head.get().dequeWrapper;
        ElementWrapper<E> e = head.get().elementWrapper;
        
        decrementAndReport(dequeKey, d); // <-- trigger report of new positions
        
        return Optional.of(e.value);
    }
    
    /**
     * Will remove and return the head of the mapped deque.<p>
     * 
     * If the deque became empty, it will be removed. No other side effects
     * happen: position changes are not reported.
     * 
     * @param dequeKey the key
     * 
     * @return the {@code DequeWrapper<E>} and {@code ElementWrapper<E>} put
     *         together into an {@code Entry<E, E>}, is empty if remove
     *         operation was not successful
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     */
    private Optional<Entry<E, E>> doRemoveFirst(K dequeKey)
    {
        Optional<DequeWrapper<E>> d1 = getDoNotCreateDeque(dequeKey);
        
        // We found no deque?
        if (!d1.isPresent()) {
            LOGGER.finer(() -> "Asked to remove first of key " + dequeKey  + "; but I found no such deque in store.");
            return Optional.empty();
        }
        
        final DequeWrapper<E> d2 = d1.get();
        
        try
        {
            final ElementWrapper<E> head = d2.deque.remove(); // <-- possible NoSuchElementException
            
            if (head.hasBeenRemoved.compareAndSet(false, true))
                return Optional.of(new Entry<>(d2, head));
            else
                return doRemoveFirst(dequeKey);
        }
        catch (NoSuchElementException e) {
            LOGGER.finer(() -> "Asked to remove first of key " + dequeKey  + "; but the deque was empty.");
            removeEmptyQueue(dequeKey, d2);
            return Optional.empty();
        }
    }
    
    /**
     * Will remove and return the head of the mapped deque, if the element
     * matches the predicate.<p>
     * 
     * If the deque became empty, it will be removed. No other side effects
     * happen: position changes are not reported.
     * 
     * @param dequeKey the key
     * @param predicate the predicate
     * 
     * @return the {@code DequeWrapper<E>} and {@code ElementWrapper<E>} put
     *         together into an {@code Entry<E, E>}, is empty if remove
     *         operation was not successful
     */
    private Optional<Entry<E, E>> doRemoveFirstIf(K dequeKey, Predicate<? super E> predicate)
    {
        Optional<DequeWrapper<E>> d1 = getDoNotCreateDeque(dequeKey);
        
        // We found no deque?
        if (!d1.isPresent()) {
            LOGGER.finer(() -> "Asked to remove first of key " + dequeKey  + "; but I found no such deque in store.");
            return Optional.empty();
        }
        
        final DequeWrapper<E> d2 = d1.get();
        final ElementWrapper<E> head = d2.deque.peekFirst();
        
        if (head == null)
        {
            LOGGER.finer(() -> "Asked to remove first of key " + dequeKey  + "; but the deque was empty.");
            removeEmptyQueue(dequeKey, d2);
            return Optional.empty();
        }
        
        if (predicate.test(head.value))
        {
            if (head.hasBeenRemoved.compareAndSet(false, true) && d2.deque.remove(head))
            {
                if (d2.deque.isEmpty())
                    removeEmptyQueue(dequeKey, d2);
                
                return Optional.of(new Entry<>(d2, head));
            }
            else
                return doRemoveFirstIf(dequeKey, predicate);
        }
        else
            return Optional.empty();
    }
    
    /**
     * Will insert the element at the front of the mapped deque.<p>
     * 
     * If the deque does not exist, it will be created. There are no other
     * side effects: position changes are not reported.
     * 
     * @param dequeKey the key that maps (or will be mapped) to a deque
     * @param element the element to be added
     * 
     * @return the deque the element was added to
     */
    private DequeWrapper<E> doAddFirst(K dequeKey, E element)
    {
        DequeWrapper<E> w = getOrCreateDeque(dequeKey);
        
        ElementWrapper<E> v = new ElementWrapper<>(element, 1);
        w.deque.addFirst(v);
        
        return w;
    }
    
    /**
     * Will increment the provided deque by {@code 1} and report possible
     * position changes to position-aware elements.
     * 
     * @param deque the deque
     */
    private void incrementAndReport(DequeWrapper<E> deque)
    {
        deque.counter.increment();
        deque.needPositionReports = true;
        reportPositionChanges();
    }
    
    /**
     * Will decrement the provided deque by {@code 1} and report possible
     * position changes to position-aware elements.
     * 
     * @param dequeKey the key
     * @param expected the expected deque to be found using the key
     */
    private void decrementAndReport(K dequeKey, DequeWrapper<E> expected)
    {
        expected.counter.decrement();
        
        // Deque MIGHT have become empty:
        removeQueueIfEmpty(dequeKey, expected);
        
        expected.needPositionReports = true;
        reportPositionChanges();
    }
    
    /**
     * Will get an already existing concurrent deque, or create one if there was
     * no such deque mapped to the provided key.
     * 
     * @param dequeKey the deque key
     * 
     * @throws NullPointerException if dequeKey is {@code null}
     * 
     * @returns always a non-null {@code DequeWrapper<E>}
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     */
    private DequeWrapper<E> getOrCreateDeque(K dequeKey) {
        Objects.requireNonNull(dequeKey, "dequeKey is null");
        return DEQUES.computeIfAbsent(dequeKey, ignoredKey -> new DequeWrapper<>());
    }
    
    /**
     * Will get an already existing concurrent deque mapped to the provided
     * key.<p>
     * 
     * This method has no side effects such as creating a deque.
     * 
     * @param dequeKey the deque key
     * 
     * @throws NullPointerException if {@code dequeKey} is {@code null}
     * 
     * @returns the {@code DequeWrapper<E>}
     */
    private Optional<DequeWrapper<E>> getDoNotCreateDeque(K dequeKey) {
        Objects.requireNonNull(dequeKey, "dequeKey is null");
        return Optional.ofNullable(DEQUES.get(dequeKey));
    }
    
    /**
     * Will remove the assumed to be empty deque, if the manager currently store
     * the expected deque.<p>
     * 
     * The size of the deque is not evaluated. But as a necessary safe guard,
     * elements left behind in the removed deque will we enqueued using
     * {@linkplain #addFirst(K, E)}. This should in practice happen very rarely.
     * 
     * @param dequeKey the key
     * @param expected the deque expected to be mapped to the provided key
     * 
     * @return {@code true} if we found the deque and removed it, otherwise
     *         {@code false}
     * 
     * @throws NullPointerException if any of the arguments are {@code null}
     */
    private boolean removeEmptyQueue(K dequeKey, DequeWrapper<E> expected)
    {
        final boolean success = DEQUES.remove(dequeKey, expected); // <-- possible NPE
        
        if (success)
            LOGGER.finer(() -> "Successfully removed empty deque mapped to: " + dequeKey + ".");
        else
            LOGGER.finer(() -> "Tried to but did not succeed (deque already removed or new deque already assigned) in removal of empty deque mapped to: " + dequeKey + ".");
        
        expected.counter.reset();
        
        /*
         * What harm would the following line of code do if the queue really is empty ;)
         * BUT it could be in the concurrent world we live in that the queue actually has some elements.
         */
        expected.deque.forEach(e -> { addFirst(dequeKey, e.value); });
        
        return success;
    }
    
    /**
     * Will inspect the actual known size of the queue and remove him only if he
     * was found to be empty.<p>
     * 
     * This method will try to run his job asynchronously, if an {@code
     * ExecutorService} was provided during construction of the manager.
     * 
     * @param queueKey the key
     * @param expected expected deque
     * 
     * @throws NullPointerException if any argument is {@code null}
     */
    private void removeQueueIfEmpty(K queueKey, DequeWrapper<E> expected)
    {
        tryRunAsync(() -> {
            if (sizeOf(queueKey) <= 0L)
                removeEmptyQueue(queueKey, expected);
        });
    }
    
    private void tryRunAsync(Runnable job)
    {
        if (workers != null) {
            workers.execute(job);
        }
        else {
            job.run();
        }
    }
    
    private <T> Future<T> tryRunAsync(Callable<T> job)
    {
        if (workers != null) {
            return workers.submit(job);
        }
        else
        {
            try {
                T result = job.call();
                return CompletedFuture(result, Optional.empty());
            } catch (Exception e) {
                return CompletedFuture(null, Optional.of(e));
            }           
        }
    }
    
    private final Lock isReporting = new ReentrantLock();
    
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
     * run. If such a job is already running, another one will be scheduled to
     * run in sequence as soon as the ongoing job is finished. At most one such
     * work will be scheduled.
     */
    private void reportPositionChanges()
    {
        trace("reportPositionChanges");
        
        if (workers == null)
            return;
        
        administrator.execute(() ->
        {
            if (isReporting.tryLock())
            {
                try
                {
                    while (reportWorkScheduled || needToReportPositions()) thenReport:
                    {
                        reportWorkScheduled = false;
                        
                        Stream<DequeWrapper<E>> dirtyDeques = dequesThatNeedReporting();

                        List<Callable<DequeWrapper<E>>> callables = dirtyDeques.map((d) -> { return (Callable<DequeWrapper<E>>) () -> {
                            /* Worker's task: */ doReport(d);
                            return null;
                        }; }).collect(Collectors.toList());

                        // Admin's task: submit to executor and wait for all async workers to finish:
                        try {
                            workers.invokeAll(callables);
                        }
                        catch (InterruptedException e) {
                            LOGGER.log(Level.WARNING, "Interrupted while waiting for reporting jobs to finish.", e);
                        }
                    }
                }
                finally {
                    isReporting.unlock();
                }
            }
            else
                reportWorkScheduled = true;
        });
    }
    
    /**
     * @return {@code true} if at least one deque has been marked to need
     *         position reports, otherwise {@code false}
     */
    private boolean needToReportPositions() {
        return DEQUES.values().stream().anyMatch(q -> q.needPositionReports);
    }
    
    /**
     * @return all deques that has been marked to need position reports
     */
    private Stream<DequeWrapper<E>> dequesThatNeedReporting() {
        return DEQUES.values().stream().filter(q -> q.needPositionReports);
    }
    
    /**
     * Will report a change of position to all the provided deque's
     * position-aware elements.
     */
    private void doReport(DequeWrapper<E> deque)
    {
        if (!deque.needPositionReports)
            return;
        
        deque.needPositionReports = false;
        long index = 0L;
        
        for (ElementWrapper<E> e : deque.deque)
        {
            ++index;
            
            if (e.value instanceof PositionAware && e.lastKnownPosition != index)
            {
                try {
                    ((PositionAware) e.value).setPosition(index);
                }
                catch (RuntimeException ex) {
                    LOGGER.log(Level.WARNING, "Caught an exception while trying to report a new position!", ex);
                }
                
                e.lastKnownPosition = index;
            }
        }
    }
    
    
    
    /*
     *  ----------------
     * | POSITION AWARE |
     *  ----------------
     */
    
    /**
     * Used as a marker and as a callback by {@linkplain ConcurrentDequeManager}
     * to provide the elements of his deques a chance to know when their
     * position in the deque changes.<p>
     * 
     * The first position is not 0, but 1. Just like in real life that is.
     * Likewise, the last position is the size of the deque.<p>
     * 
     * It is expected that position-aware objects live in a highly concurrent
     * environment and therefore that the position reported is only weakly
     * consistent. A position-aware object is not notified when he enter or
     * when he leave a deque. The only thing that is reported is when his
     * already established position in a deque <i>changes</i>.
     * 
     * @author Martin Andersson (webmaster at martinandersson.com)
     */
    @FunctionalInterface
    public interface PositionAware
    {
        /**
         * Called when the element has moved in the deque to a new position.
         * 
         * @param position is never {@code < 1}
         * 
         * @see PositionAware
         * @see ConcurrentDequeManager
         */
        void setPosition(long position);
    }
    
    
    
    /*
     *  ------------
     * | CONTAINERS |
     *  ------------
     */
    
    private static class DequeWrapper<V>
    {
        final ConcurrentLinkedDeque<ElementWrapper<V>> deque = new ConcurrentLinkedDeque<>();
        final LongAdder counter = new LongAdder();
        
        /**
         * Flag to mark that a deque has undergone a structural modification
         * such that the deque's elements could need a new position changed
         * report.
         */
        volatile boolean needPositionReports = false;
        
        // Default impl. of equals and hashCode using == is just what we want!
    }
    
    private static class ElementWrapper<E>
    {
        final E value;
        
        final AtomicBoolean hasBeenRemoved = new AtomicBoolean();
        
        final long initialPosition;
        volatile long lastKnownPosition;
        
        ElementWrapper(E value) {
            this(value, -1);
        }
        
        ElementWrapper(E value, long initialPosition) {
            this.value = Objects.requireNonNull(value, "element is null");
            this.initialPosition = initialPosition;
            this.lastKnownPosition = initialPosition;
        }
        
        @Override
        public int hashCode() {
            return value.hashCode(); }
        
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            
            if (obj == null)
                return false;
            
            if (ElementWrapper.class != obj.getClass())
                return false;
            
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