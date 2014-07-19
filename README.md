### ConcurrentDequeManager

A concurrent and highly performant Java manager composed of many `ConcurrentLinkedDeque`, each deque mapped by a key. All deques are completely transparent to the client.

Next stop, read:   
[./src/main/java/martinandersson/com/lib/concurrent/ConcurrentDequeManager.java](https://github.com/MartinanderssonDotcom/ConcurrentDequeManager/blob/master/src/main/java/martinandersson/com/lib/concurrent/ConcurrentDequeManager.java)

### Features

* Awesome API.  
   `long initialPosition = manager.addLast("BID Ipads", 10_000);`  
   `Optional<Integer> matched = manager.removeFirstIf("BID Ipads", Predicate.isEqual(10_000));`

* Amount of deques grow and shrink on demand
* Elements may optionally receive position change notifications if they implement `ConcurrentDequeManager.PositionAware`.
* Lock-free

### Users

* [martinandersson.com/livechat/](http://www.martinandersson.com/livechat/ "Author's homepage"): Used as a web user queue system when I am too busy or not online.
