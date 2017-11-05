package marnikitta.gutsdb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public final class GutsLog implements AutoCloseable {
  private final FileChannel fileChannel;

  private Index index = null;

  public GutsLog(Path path) throws IOException {
    this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
  }

  public Iterator<RichBlock> blockIterator(ByteBuffer buffer) throws IOException {
    return new Iterator<RichBlock>() {
      private final Iterator<Map.Entry<ByteBuffer, Index.BlockHandle>> iterator = index().iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public RichBlock next() {
        if (hasNext()) {
          try {
            final Map.Entry<ByteBuffer, Index.BlockHandle> next = iterator.next();
            final Index.BlockHandle handle = next.getValue();
            buffer.reset();
            buffer.limit(handle.size());

            fileChannel.position(handle.offset());
            fileChannel.read(buffer);
            buffer.flip();

            return new RichBlock(next.getKey(), new Block(buffer));

          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public Index index() throws IOException {
    if (index == null) {
      fetchMeta();
    }

    return index;
  }

  private void fetchMeta() throws IOException {
    final ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.BYTES);
    fileChannel.position(fileChannel.size() - Footer.BYTES);
    fileChannel.read(footerBuffer);
    footerBuffer.flip();
    final Footer footer = new Footer(footerBuffer);

    final ByteBuffer indexBuffer = ByteBuffer.allocate(footer.index().size());
    fileChannel.position(footer.index().offset());
    fileChannel.read(indexBuffer);
    indexBuffer.flip();
    this.index = new Index(indexBuffer);
  }

  @Override
  public void close() throws Exception {
    fileChannel.close();
  }

  private static final class Footer {
    public static final int BYTES = Index.BlockHandle.BYTES;

    private final Index.BlockHandle index;

    public Footer(ByteBuffer from) {
      this(new Index.BlockHandle(from));
    }

    public Footer(Index.BlockHandle index) {
      this.index = index;
    }

    private int spill(ByteBuffer to) {
      return index.spill(to);
    }

    public Index.BlockHandle index() {
      return index;
    }
  }

  public static final class RichBlock {
    private final ByteBuffer ceilKey;
    private final Block block;

    public RichBlock(ByteBuffer ceilKey, Block block) {
      this.ceilKey = ceilKey;
      this.block = block;
    }

    public ByteBuffer ceilKey() {
      return ceilKey;
    }

    public Block block() {
      return block;
    }
  }
}
