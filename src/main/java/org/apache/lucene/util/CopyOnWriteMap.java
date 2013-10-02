package org.apache.lucene.util;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A simple CopyOnWrite Map implementation backed by a {@link ImmutableMap}
 */
public final class CopyOnWriteMap<K, V> implements Map<K, V> {
  private final Object sync = new Object();
  private volatile ImmutableMap<K, V> map = ImmutableMap.of();
  
  public CopyOnWriteMap() {
  }
 
  public boolean isEmpty() {
    return map.isEmpty();
  }

  public int size() {
    return map.size();
  }

  public Collection<V> values() {
    return map.values();
  }

  public Set<K> keySet() {
    return map.keySet();
  }

  public Set<Entry<K, V>> entrySet() {
    return map.entrySet();
  }

  public V get(Object key) {
    return map.get(key);
  }

  public boolean containsValue(Object value) {
    return map.containsValue(value);
  }

  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  public void clear() {
    synchronized (sync) {
      map = ImmutableMap.of();
    }
  }
  

  public void putAll(Map<? extends K, ? extends V> map) {
    synchronized (sync) {
        ImmutableMap.Builder<K,V> builder = ImmutableMap.builder();
        map = builder.putAll(this.map).putAll(map).build();
    }
  }

  public V remove(Object o) {
    throw new UnsupportedOperationException();
  }

 
  public V put(K k, V v) {
    synchronized (sync) {
      ImmutableMap.Builder<K,V> builder = ImmutableMap.builder();
      map = builder.putAll(map).put(k, v).build();
      return v;
    }
  }

  public String toString() {
    return map.toString();
  }
}
