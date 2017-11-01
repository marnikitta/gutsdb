package marnikitta.gutsdb.block;


import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class Block implements Iterable<Block.Entry> {
  private final ByteBuffer buffer;

  public Block(ByteBuffer backingBuffer) {
    this.buffer = backingBuffer.slice();
  }

  public static int spill(Map<ByteBuffer, ByteBuffer> source, ByteBuffer destination) {
    int[] written = {0};

    source.forEach((key, value) -> {

      destination.putInt(key.limit());
      written[0] += Integer.BYTES;

      destination.putInt(value.limit());
      written[0] += Integer.BYTES;

      key.rewind();
      destination.put(key);
      written[0] += key.limit();

      value.rewind();
      destination.put(value);
      written[0] += value.limit();
    });

    return written[0];
  }

  @Override
  public Iterator<Entry> iterator() {
    return new Iterator<Entry>() {
      {
        buffer.rewind();
      }

      @Override
      public boolean hasNext() {
        return buffer.hasRemaining();
      }

      @Override
      public Entry next() {
        if (hasNext()) {
          final int keySize = buffer.getInt();
          final int valueSize = buffer.getInt();

          final int realLimit = buffer.limit();

          buffer.limit(buffer.position() + keySize);
          final ByteBuffer key = buffer.slice();
          buffer.position(buffer.position() + keySize);


          buffer.limit(buffer.position() + valueSize);
          final ByteBuffer value = buffer.slice();

          buffer.position(buffer.position() + valueSize);
          buffer.limit(realLimit);

          return new Entry(key, value);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public Stream<Entry> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  public Map<ByteBuffer, ByteBuffer> asMap() {
    final Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
    forEach(result::put);
    return result;
  }

  public boolean isEmpty() {
    return buffer.limit() == 0;
  }

  public boolean containsKey(Object key) {
    return stream().anyMatch(e -> e.key().equals(key));
  }

  public ByteBuffer get(Object key) {
    return stream()
      .filter(e -> e.key().equals(key))
      .findAny().map(Entry::value)
      .orElse(null);
  }

  public Set<ByteBuffer> keySet() {
    return stream().map(Entry::key).collect(toSet());
  }

  public Collection<ByteBuffer> values() {
    return stream().map(Entry::value).collect(toList());
  }

  public void forEach(BiConsumer<? super ByteBuffer, ? super ByteBuffer> action) {
    forEach(e -> action.accept(e.key(), e.value()));
  }

  public static final class Entry {
    private final ByteBuffer key;
    private final ByteBuffer value;

    public Entry(ByteBuffer key, ByteBuffer value) {
      this.key = key;
      this.value = value;
    }

    public ByteBuffer key() {
      return key;
    }

    public ByteBuffer value() {
      return value;
    }
  }


  public static void main(String... args) {
    final Map<ByteBuffer, ByteBuffer> myTable = Stream
      .generate(UUID::randomUUID)
      .limit(1000)
      .map(UUID::toString)
      .map(String::getBytes)
      .map(ByteBuffer::wrap)
      .collect(toMap(Function.identity(), Function.identity()));

    final ByteBuffer allocate = ByteBuffer.allocate(100000);
    spill(myTable, allocate);
    allocate.flip();

    final Block block = new Block(allocate);

    final Map<ByteBuffer, ByteBuffer> map = block.asMap();

    System.out.println(map.equals(myTable));
  }
}
