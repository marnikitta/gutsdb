package marnikitta.gutsdb.block;

import marnikitta.gutsdb.Block;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class BlockTest {

  @Test
  public void testSpillRead() {
    final Map<String, String> source = Stream.generate(UUID::randomUUID)
      .map(UUID::toString)
      .limit(1000)
      .collect(toMap(Function.identity(), Function.identity()));

    final Map<ByteBuffer, ByteBuffer> myTable = source.entrySet().stream()
      .collect(toMap(
        s -> ByteBuffer.wrap(s.getKey().getBytes()),
        s -> ByteBuffer.wrap(s.getValue().getBytes())
      ));

    final ByteBuffer allocate = ByteBuffer.allocate(100000);
    Block.spill(myTable, allocate);
    allocate.flip();

    final Block block = new Block(allocate);

    final Map<String, String> collect = block.asMap()
      .entrySet().stream()
      .collect(toMap(
        b -> new String(toArray(b.getKey())),
        b -> new String(toArray(b.getKey()))
      ));

    Assert.assertEquals(collect, source);
  }

  private byte[] toArray(ByteBuffer byteBuffer) {
    byteBuffer.mark();
    final byte[] result = new byte[byteBuffer.remaining()];
    byteBuffer.get(result);
    byteBuffer.reset();
    return result;
  }
}