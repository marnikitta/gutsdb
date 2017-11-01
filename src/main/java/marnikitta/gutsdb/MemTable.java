package marnikitta.gutsdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

public final class MemTable implements Map<ByteBuffer, ByteBuffer> {
  private final ConcurrentMap<ByteBuffer, ByteBuffer> currentTable = new ConcurrentSkipListMap<>();
  private final int keySizeLimit;

  public MemTable(int keySizeLimit) {
    this.keySizeLimit = keySizeLimit;
  }

  @Override
  public int size() {
    return currentTable.size();
  }

  @Override
  public boolean isEmpty() {
    return currentTable.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return currentTable.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return currentTable.containsValue(value);
  }

  @Override
  public ByteBuffer get(Object key) {
    return currentTable.get(key);
  }

  @Override
  public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
    return currentTable.put(key, value);
  }

  @Override
  public ByteBuffer remove(Object key) {
    return currentTable.remove(key);
  }

  @Override
  public void putAll(Map<? extends ByteBuffer, ? extends ByteBuffer> m) {
    currentTable.putAll(m);
  }

  @Override
  public void clear() {
    currentTable.clear();
  }

  @Override
  public Set<ByteBuffer> keySet() {
    return currentTable.keySet();
  }

  @Override
  public Collection<ByteBuffer> values() {
    return currentTable.values();
  }

  @Override
  public Set<Entry<ByteBuffer, ByteBuffer>> entrySet() {
    return currentTable.entrySet();
  }
}
