/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zaxxer.hikari.util;

import static java.lang.Thread.yield;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_REMOVED;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_RESERVED;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge
 *
 * @param <T> the templated type to store in the bag
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

   /**
    * 这个就是 能够被多线程共享的偷取队列 跟 FJ思路不同 fj是每个线程能直接访问到其他线程的工作队列
    * 而这里是先尝试从本地线程变量获取 没有的话再尝试从 共享队列中获取
    */
   private final CopyOnWriteArrayList<T> sharedList;
   private final boolean weakThreadLocals;

   private final ThreadLocal<List<Object>> threadList;
   /**
    * 当背包状态发生变更时的监听器
    */
   private final IBagStateListener listener;
   /**
    * 当前有多少等待者
    */
   private final AtomicInteger waiters;
   /**
    * 是否被关闭
    */
   private volatile boolean closed;
   /**
    * 同步队列 特性是 每当前有一个线程设置元素 另一个等待元素的线程才会被唤醒
    */
   private final SynchronousQueue<T> handoffQueue;

   /**
    * 应该是 并发容器中的数据实体
    */
   public interface IConcurrentBagEntry
   {
      int STATE_NOT_IN_USE = 0;
      int STATE_IN_USE = 1;
      int STATE_REMOVED = -1;
      int STATE_RESERVED = -2;

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   public interface IBagStateListener
   {
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    * 通过一个监听器对象来构建并发包
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      // 判断是否需要使用 WeakReference
      this.weakThreadLocals = useWeakThreadLocals();

      this.handoffQueue = new SynchronousQueue<>(true);
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         // 这里使用普通的list 进行初始化
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         // 创建一个 FastList<IConcurrentBagEntry> 对象
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    *
    * 从包中借用一个对象
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first  在初始化时 在 ThreadLocal 中设置了一个list 对象
      final List<Object> list = threadList.get();
      // 如果本地线程变量以初始化
      for (int i = list.size() - 1; i >= 0; i--) {
         // 从后往前获取对象
         final Object entry = list.remove(i);
         // weakThreadLocals 对应到 ArrayList  否则对应 FastList
         @SuppressWarnings("unchecked")
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         // 将获取的对象状态修改成 in_use 因为使用本地线程变量所以没有并发问题
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
            return bagEntry;
         }
      }

      // Otherwise, scan the shared list ... then poll the handoff queue
      // 增加当前等待borrow 的线程
      final int waiting = waiters.incrementAndGet();
      try {
         // 如果能从 共享队列中获取到 BagEntry 减少等待对象
         for (T bagEntry : sharedList) {
            // 因为本身使用 CopyOnWriter 在读取时会有脏读问题 所以这里配合CAS 来解决
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  // 需要通知其他等待者 共享队列中某个元素被窃取了
                  listener.addBagItem(waiting - 1);
               }
               return bagEntry;
            }
         }

         // 这里好像有点点问题 如果 waiter 从2 连续变成 3 4 之后 成为3的线程 从sharedList中获取到元素 变回2 那么触发 listener.addBagItem(2) 是对的 而另一个就变成
         // listener.addBagItem(4)  这里就跳过了3
         listener.addBagItem(waiting);

         timeout = timeUnit.toNanos(timeout);
         do {
            // 获取当前时间
            final long start = currentTime();
            // 从同步队列中阻塞获取
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
            // 超时返回null
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            // 代表还有多少剩余超时时间
            timeout -= elapsedNanos(start);
            // 因为可能获取到了bag 对象 CAS 操作却失败了就需要自旋  该bag对象适合与并发没有这么激烈的情况
         } while (timeout > 10_000);

         // 代表在规定的时间内还是没有获取到对象 返回 null
         return null;
      }
      finally {
         waiters.decrementAndGet();
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * 将目标对象归还到 bag中
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry)
   {
      bagEntry.setState(STATE_NOT_IN_USE);

      for (int i = 0; waiters.get() > 0; i++) {
         // 第一种情况 针对 sharedList 中该对象刚好被其他线程访问到 这时不用做任何操作 因为已经被其他线程持有了
         // 第二种情况 该对象没有在sharedList中被访问到 就尝试唤醒阻塞在同步队列的线程
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         // 当i 的长度超过 0xff 后 该条件会变成false
         else if ((i & 0xff) == 0xff) {
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            // 告诉时间调度器将cpu 让给其他线程
            yield();
         }
      }

      // handoffQueue.offer(bagEntry) == false 代表该对象当前没有尝试获取元素的线程 就添加到当前线程对应的队列中 方便下次快速获取
      // 代表每个线程的任务数量在50 以内都会使用 本地线程进行存储 超过50个时 就需要其他线程来分摊计算 就是用工作窃取队列 CopyOnWriter
      final List<Object> threadLocalList = threadList.get();
      if (threadLocalList.size() < 50) {
         // 为什么使用FastList 就可以不用 WeakReference 了 他们本质内部都是一个数组
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry)
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }

      // 每个元素都是被放在工作窃取队列中的 如果获取不到就通过同步队列进行阻塞 当获取到任务处理完并进行归还时
      // 如果发现没有等待任务的线程就 设置到本地线程变量中 这时该对象的引用还存在于 CopyOnWriter数组 中 只是方便其他线程能通过 ThreadLocalMap 快速获取
      sharedList.add(bagEntry);

      // spin until a thread takes it or none are waiting
      // 如果当前有等待者 必须要等待者获取完任务后才返回
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) {
         yield();
      }
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * 必须在本节点处在 in_use 的状态才能移除
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry)
   {
      // 下面条件等同 (! (condition1 || condition2 || condition3))  代表 不是从in_use 或者 reserved 或者 close 状态下触发就返回false
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      // 实际上该对象只会往这个容器中插入 或删除元素 其他辅助容器 只是加速线程访问 或者协调线程 内部引用指向同一对象
      // 因为这里没有主动去本地线程变量移除元素 所以需要他能被自动回收 也就使用到 WeakReference 但是 FastLst 内元素为什么不需要WeakReference 包裹
      final boolean removed = sharedList.remove(bagEntry);
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final List<T> list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * 将某个元素从 not_in_use 变成 reserved(保留状态) 这样就不能被使用
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry)
   {
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED);
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    * 接触保留状态
    * @param bagEntry the item to unreserve
    */
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      int count = 0;
      for (IConcurrentBagEntry e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   /**
    * 获取描述该包状态的数组
    * @return
    */
   public int[] getStateCounts()
   {
      final int[] states = new int[6];
      for (IConcurrentBagEntry e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    * 是否使用弱引用的依据是 该对象是否有自己的类加载器
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals()
   {
      try {
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader();
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
