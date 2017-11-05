package marnikitta.gutsdb;


import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class Block implements Iterable<Block.Entry> {
  private final ByteBuffer backingBuffer;

  public Block(ByteBuffer backingBuffer) {
    this.backingBuffer = backingBuffer;
  }

  public static int spill(Map<ByteBuffer, ByteBuffer> source, ByteBuffer destination) {
    final int[] written = {0};

    source.forEach((key, value) -> {
      final int keySize = key.remaining();
      final int valueSize = value.remaining();
      written[0] += Integer.BYTES;
      written[0] += Integer.BYTES;
      written[0] += keySize;
      written[0] += valueSize;

      destination.putInt(keySize);
      destination.putInt(valueSize);

      key.mark();
      destination.put(key);
      key.reset();

      value.mark();
      destination.put(value);
      value.reset();
    });

    return written[0];
  }

  @Override
  public Iterator<Entry> iterator() {
    return new Iterator<Entry>() {
      private final ByteBuffer iteratorBuffer = backingBuffer.asReadOnlyBuffer();

      @Override
      public boolean hasNext() {
        return iteratorBuffer.hasRemaining();
      }

      @Override
      public Entry next() {
        if (hasNext()) {
          final int keySize = iteratorBuffer.getInt();
          final int valueSize = iteratorBuffer.getInt();

          final int realLimit = iteratorBuffer.limit();

          iteratorBuffer.limit(iteratorBuffer.position() + keySize);
          final ByteBuffer key = iteratorBuffer.slice();
          iteratorBuffer.position(iteratorBuffer.position() + keySize);


          iteratorBuffer.limit(iteratorBuffer.position() + valueSize);
          final ByteBuffer value = iteratorBuffer.slice();

          iteratorBuffer.position(iteratorBuffer.position() + valueSize);
          iteratorBuffer.limit(realLimit);

          return new Entry(key, value);
        } else {
          throw new NoSuchElementException("Illegal backingBuffer format");
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
    return backingBuffer.limit() == 0;
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
}
