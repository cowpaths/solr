/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.storage;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.compress.LZ4;

public class CompressingDirectory extends FSDirectory {

  private static final Logger LOG = Logger.getLogger(CompressingDirectory.class.getName());

  /**
   * Reference to {@code com.sun.nio.file.ExtendedOpenOption.DIRECT} by reflective class and enum
   * lookup. There are two reasons for using this instead of directly referencing
   * ExtendedOpenOption.DIRECT:
   *
   * <ol>
   *   <li>ExtendedOpenOption.DIRECT is OpenJDK's internal proprietary API. This API causes
   *       un-suppressible(?) warning to be emitted when compiling with --release flag and value N,
   *       where N is smaller than the the version of javac used for compilation. For details,
   *       please refer to https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8259039.
   *   <li>It is possible that Lucene is run using JDK that does not support
   *       ExtendedOpenOption.DIRECT. In such a case, dynamic lookup allows us to bail out with
   *       UnsupportedOperationException with meaningful error message.
   * </ol>
   *
   * <p>This reference is {@code null}, if the JDK does not support direct I/O.
   */
  static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

  static {
    OpenOption option;
    try {
      final Class<? extends OpenOption> clazz =
          Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
      option =
          Arrays.stream(clazz.getEnumConstants())
              .filter(e -> e.toString().equalsIgnoreCase("DIRECT"))
              .findFirst()
              .orElse(null);
    } catch (
        @SuppressWarnings("unused")
        Exception e) {
      option = null;
    }
    ExtendedOpenOption_DIRECT = option;
  }

  static final boolean DEFAULT_USE_DIRECT_IO = false;

  static OpenOption getDirectOpenOption() {
    if (ExtendedOpenOption_DIRECT == null) {
      throw new UnsupportedOperationException(
          "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version.");
    }
    return ExtendedOpenOption_DIRECT;
  }

  private final int blockSize;
  private final ExecutorService ioExec;
  private final Path directoryPath;
  private final boolean useAsyncIO;
  private final boolean useDirectIO;

  public CompressingDirectory(
      Path path, ExecutorService ioExec, boolean useAsyncIO, boolean useDirectIO)
      throws IOException {
    super(path, FSLockFactory.getDefault());
    this.blockSize = (int) (Files.getFileStore(path).getBlockSize());
    this.ioExec = ioExec;
    directoryPath = path;
    this.useAsyncIO = useAsyncIO;
    this.useDirectIO = useDirectIO;
  }

  @Override
  public long fileLength(String name) throws IOException {
    Path path = directoryPath.resolve(name);
    File file = path.toFile();
    super.fileLength(name); // to throw NoSuchFileException -- hacky
    if (file.length() < Long.BYTES) {
      return 0;
    } else {
      try (FileInputStream in = new FileInputStream(file)) {
        byte[] bytes = in.readNBytes(Long.BYTES);
        return ByteBuffer.wrap(bytes).getLong(0);
      }
    }
  }

  /** From DirectIODirectory */
  public static final int DEFAULT_MERGE_BUFFER_SIZE = 256 * 1024;

  /**
   * We will have 2 alternating read buffers of this size. Each buffer is quite large (8M), but this
   * is off-heap memory, and recall that we are hereby entirely skipping the page cache, through
   * which all of this data would otherwise be churned.
   *
   * <p>The main reason for the large size is because iowait is quite bursty; the larger the reads,
   * the more diffusely the iowait is spread out, reducing the risk that an individual hiccup will
   * block the processing (i.e. decompressing) thread.
   */
  public static final int DEFAULT_DISK_READ_BUFFER_SIZE =
      DEFAULT_MERGE_BUFFER_SIZE << 5; // 8M buffer (large) seems optimal

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    return new DirectIOIndexOutput(
        directoryPath.resolve(name),
        name,
        blockSize,
        DEFAULT_MERGE_BUFFER_SIZE,
        ioExec,
        Integer.MAX_VALUE,
        useAsyncIO,
        useDirectIO);
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    throw new UnsupportedOperationException("CompressingDirectory does not create temp outputs");
  }

  private static class AccessibleBAOS extends ByteArrayOutputStream {
    public int transferTo(DataOutput out) throws IOException {
      out.writeBytes(this.buf, this.count);
      return this.count;
    }
  }

  private static class BytesOut extends OutputStreamDataOutput {
    private final AccessibleBAOS baos;

    public BytesOut() {
      this(new AccessibleBAOS());
    }

    private BytesOut(AccessibleBAOS baos) {
      super(baos);
      this.baos = baos;
    }

    public int transferTo(DataOutput out) throws IOException {
      return baos.transferTo(out);
    }
  }

  static final class DirectIOIndexOutput extends IndexOutput {
    private final byte[] compressBuffer = new byte[COMPRESSION_BLOCK_SIZE];
    private final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    private final ByteBuffer preBuffer;
    private final AsyncDirectWriteHelper writeHelper;
    private ByteBuffer buffer;
    private final BytesOut blockDeltas = new BytesOut();
    private int prevBlockSize = BLOCK_SIZE_ESTIMATE; // estimate 50% compression
    private boolean wroteBlock = false;
    private final ByteBuffer initialBlock;

    private long filePos;
    private boolean isOpen;

    /**
     * Creates a new instance of DirectIOIndexOutput for writing index output with direct IO
     * bypassing OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support support Direct I/O
     *     or a sufficient equivalent.
     */
    public DirectIOIndexOutput(
        Path path,
        String name,
        int blockSize,
        int bufferSize,
        ExecutorService ioExec,
        int expectLength,
        boolean useAsyncIO,
        boolean useDirectIO)
        throws IOException {
      super("DirectIOIndexOutput(path=\"" + path.toString() + "\")", name);

      // stored only to lazily compute the pathHash
      writeHelper = new AsyncDirectWriteHelper(blockSize, bufferSize, path, useDirectIO);
      buffer = writeHelper.init(0);
      preBuffer = ByteBuffer.wrap(compressBuffer);
      initialBlock = ByteBuffer.allocateDirect(blockSize + blockSize - 1).alignedSlice(blockSize);
      buffer.putLong(0);
      buffer.putInt(0);

      if (expectLength > bufferSize && useAsyncIO) {
        writeHelper.start(ioExec);
      } else {
        writeHelper.startSync();
      }
      isOpen = true;
    }

    @Override
    public void writeByte(byte b) throws IOException {
      preBuffer.put(b);
      if (!preBuffer.hasRemaining()) {
        dump();
      }
    }

    @Override
    public void writeBytes(byte[] src, int offset, int len) throws IOException {
      int toWrite = len;
      while (true) {
        final int left = preBuffer.remaining();
        if (left <= toWrite) {
          preBuffer.put(src, offset, left);
          toWrite -= left;
          offset += left;
          dump();
        } else {
          preBuffer.put(src, offset, toWrite);
          break;
        }
      }
    }

    private void dump() throws IOException {
      assert preBuffer.position() == COMPRESSION_BLOCK_SIZE;

      preBuffer.rewind();

      LZ4.compressWithDictionary(compressBuffer, 0, 0, COMPRESSION_BLOCK_SIZE, out, ht);
      int nextBlockSize = out.resetSize();
      blockDeltas.writeZInt(nextBlockSize - prevBlockSize);
      prevBlockSize = nextBlockSize;
      filePos += COMPRESSION_BLOCK_SIZE;

      preBuffer.clear();
    }

    private void flush() throws IOException {
      preBuffer.flip();
      int preBufferRemaining = preBuffer.remaining();
      if (preBufferRemaining > 0) {
        filePos += preBufferRemaining;
        LZ4.compressWithDictionary(compressBuffer, 0, 0, preBufferRemaining, out, ht);
      }
      int blockMapFooterSize = blockDeltas.transferTo(out);
      if (wroteBlock) {
        writeHelper.flush(buffer, true);
        initialBlock.putLong(0, filePos);
        initialBlock.putInt(Long.BYTES, blockMapFooterSize);
        writeHelper.write(initialBlock, 0);
      } else {
        if (filePos > 0) {
          buffer.putLong(0, filePos);
          buffer.putInt(Long.BYTES, blockMapFooterSize);
        } else {
          assert filePos == 0 && buffer.position() == Long.BYTES + Integer.BYTES;
          buffer.rewind();
          buffer.limit(0);
        }
        writeHelper.flush(buffer, true);
      }
    }

    private final SizeTrackingDataOutput out = new SizeTrackingDataOutput();

    private void writeBlock() throws IOException {
      if (!wroteBlock) {
        wroteBlock = true;
        buffer.rewind();
        int restoreLimit = buffer.limit();
        buffer.limit(initialBlock.limit());
        initialBlock.put(buffer);
        initialBlock.rewind();
        buffer.limit(restoreLimit);
      }
      // we need to rewind, as we have to write full blocks (we truncate file later):
      buffer.rewind();
      buffer = writeHelper.write(buffer);
    }

    @Override
    public long getFilePointer() {
      return filePos + preBuffer.position();
    }

    @Override
    public long getChecksum() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      if (isOpen) {
        isOpen = false;
        try (writeHelper) {
          // flush and close channel
          flush();
        }
      }
    }

    private class SizeTrackingDataOutput extends DataOutput {
      private int size = 0;

      private int resetSize() {
        int ret = size;
        size = 0;
        return ret;
      }

      @Override
      public void writeByte(byte b) throws IOException {
        size++;
        shouldWriteBytes(1);
        buffer.put(b);
      }

      @Override
      public void writeBytes(byte[] b, int offset, int length) throws IOException {
        size += length;
        do {
          int shouldWriteBytes = shouldWriteBytes(length);
          buffer.put(b, offset, shouldWriteBytes);
          offset += shouldWriteBytes;
          length -= shouldWriteBytes;
        } while (length > 0);
      }

      private int shouldWriteBytes(int wantWriteBytes) throws IOException {
        int remaining = buffer.remaining();
        if (remaining >= wantWriteBytes) {
          return wantWriteBytes;
        } else if (remaining > 0) {
          return remaining;
        }
        writeBlock();
        return Math.min(wantWriteBytes, buffer.remaining());
      }
    }
  }

  public static final int COMPRESSION_BLOCK_SHIFT = 18;
  public static final int COMPRESSION_BLOCK_SIZE = 1 << COMPRESSION_BLOCK_SHIFT; // 256k
  public static final int COMPRESSION_BLOCK_MASK_LOW = COMPRESSION_BLOCK_SIZE - 1;
  public static final int BLOCK_SIZE_ESTIMATE =
      COMPRESSION_BLOCK_SIZE >> 1; // estimate 50% compression;

  private static final int MIN_MATCH = 4;

  /**
   * Copied from {@link LZ4#decompress(DataInput, int, byte[], int)} because it's faster
   * decompressing from byte[] than from {@link DataInput}.
   */
  public static int decompress(
      final byte[] compressed, int srcPos, final int decompressedLen, final byte[] dest, int dOff)
      throws IOException {
    final int destEnd = dOff + decompressedLen;

    do {
      // literals
      final int token = compressed[srcPos++] & 0xFF;
      int literalLen = token >>> 4;

      if (literalLen != 0) {
        if (literalLen == 0x0F) {
          byte len;
          while ((len = compressed[srcPos++]) == (byte) 0xFF) {
            literalLen += 0xFF;
          }
          literalLen += len & 0xFF;
        }
        System.arraycopy(compressed, srcPos, dest, dOff, literalLen);
        srcPos += literalLen;
        dOff += literalLen;
      }

      if (dOff >= destEnd) {
        break;
      }

      // matchs
      final int matchDec = ((compressed[srcPos++] & 0xFF) | (compressed[srcPos++] << 8)) & 0xFFFF;
      assert matchDec > 0;

      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        int len;
        while ((len = compressed[srcPos++]) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      matchLen += MIN_MATCH;

      // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
      final int fastLen = (matchLen + 7) & 0xFFFFFFF8;
      if (matchDec < matchLen || dOff + fastLen > destEnd) {
        // overlap -> naive incremental copy
        for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff) {
          dest[dOff] = dest[ref];
        }
      } else {
        // no overlap -> arraycopy
        System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);
        dOff += matchLen;
      }
    } while (dOff < destEnd);

    return srcPos;
  }
}
