package marnikitta.gutsdb;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class Index implements Iterable<Map.Entry<ByteBuffer, Index.BlockHandle>> {
  private final NavigableMap<ByteBuffer, BlockHandle> map;

  public Index(ByteBuffer byteBuffer) {
    final NavigableMap<ByteBuffer, BlockHandle> result = new TreeMap<>();

    while (byteBuffer.hasRemaining()) {
      final int keySize = byteBuffer.getInt();

      final int realLimit = byteBuffer.limit();
      byteBuffer.limit(byteBuffer.position() + keySize);
      // TODO: 11/5/17 Copy?
      final ByteBuffer key = byteBuffer.slice();
      byteBuffer.limit(realLimit);

      result.put(key, new BlockHandle(byteBuffer));
    }

    this.map = result;
  }

  @Override
  public Iterator<Map.Entry<ByteBuffer, BlockHandle>> iterator() {
    return map.entrySet().iterator();
  }

  public int spill(ByteBuffer destination) {
    final int[] written = {0};
    map.forEach((key, handle) -> {
      final int keySize = key.remaining();
      destination.putInt(keySize);
      written[0] += Integer.BYTES;

      key.mark();
      destination.put(key);
      key.reset();
      written[0] += keySize;

      written[0] += handle.spill(destination);
    });
    return written[0];
  }

  public static final class BlockHandle {
    public static final int BYTES = Long.BYTES + Long.BYTES;
    private final long offset;
    private final int size;

    public BlockHandle(ByteBuffer from) {
      this(from.getLong(), from.getInt());
    }

    public BlockHandle(long offset, int size) {
      this.offset = offset;
      this.size = size;
    }

    public int spill(ByteBuffer to) {
      to.putLong(offset);
      to.putInt(size);
      return BYTES;
    }

    public long offset() {
      return offset;
    }

    public int size() {
      return size;
    }
  }
}
