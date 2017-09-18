package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Compressor;
/**
 * A {@link Compressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class CSSZlibCompressor implements Compressor {

  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;

  private long stream;
  private CompressionLevel level;
  private CompressionStrategy strategy;
  private final CompressionHeader windowBits;
  private int directBufferSize;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private Buffer uncompressedDirectBuf = null;
  private int uncompressedDirectBufOff = 0, uncompressedDirectBufLen = 0;
  private boolean keepUncompressedBuf = false;
  private Buffer compressedDirectBuf = null;
  private boolean finish, finished;

  /**
   * The compression level for zlib library.
   */
  public static enum CompressionLevel {
    /**
     * Compression level for no compression.
     */
    NO_COMPRESSION (0),
    
    /**
     * Compression level for fastest compression.
     */
    BEST_SPEED (1),
    
    /**
     * Compression level for best compression.
     */
    BEST_COMPRESSION (9),
    
    /**
     * Default compression level.
     */
    DEFAULT_COMPRESSION (-1);
    
    
    private final int compressionLevel;
    
    CompressionLevel(int level) {
      compressionLevel = level;
    }
    
    int compressionLevel() {
      return compressionLevel;
    }
  };
  
  /**
   * The compression level for zlib library.
   */
  public static enum CompressionStrategy {
    /**
     * Compression strategy best used for data consisting mostly of small
     * values with a somewhat random distribution. Forces more Huffman coding
     * and less string matching.
     */
    FILTERED (1),
    
    /**
     * Compression strategy for Huffman coding only.
     */
    HUFFMAN_ONLY (2),
    
    /**
     * Compression strategy to limit match distances to one
     * (run-length encoding).
     */
    RLE (3),

    /**
     * Compression strategy to prevent the use of dynamic Huffman codes, 
     * allowing for a simpler decoder for special applications.
     */
    FIXED (4),

    /**
     * Default compression strategy.
     */
    DEFAULT_STRATEGY (0);
    
    
    private final int compressionStrategy;
    
    CompressionStrategy(int strategy) {
      compressionStrategy = strategy;
    }
    
    int compressionStrategy() {
      return compressionStrategy;
    }
  };

  /**
   * The type of header for compressed data.
   */
  public static enum CompressionHeader {
    /**
     * No headers/trailers/checksums.
     */
    NO_HEADER (-15),
    
    /**
     * Default headers/trailers/checksums.
     */
    DEFAULT_HEADER (15),
    
    /**
     * Simple gzip headers/trailers.
     */
    GZIP_FORMAT (31);

    private final int windowBits;
    
    CompressionHeader(int windowBits) {
      this.windowBits = windowBits;
    }
    
    public int windowBits() {
      return windowBits;
    }
  }
  
    static {  
        try { 
            System.loadLibrary("csszjni");
            initIDs();
        } catch (Throwable e) {  
            throw new RuntimeException("load libcsszjni.so error!", e);  
        }
    }
  
  protected final void construct(CompressionLevel level, CompressionStrategy strategy,
      CompressionHeader header, int directBufferSize) {
  }

  /**
   * Creates a new compressor with the default compression level.
   * Compressed data will be generated in ZLIB format.
   */
  public CSSZlibCompressor() {
     this(CompressionLevel.DEFAULT_COMPRESSION,
          CompressionStrategy.DEFAULT_STRATEGY,
          CSSZlibCompressor.CompressionHeader.GZIP_FORMAT, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /** 
   * Creates a new compressor using the specified compression level.
   * Compressed data will be generated in ZLIB format.
   * 
   * @param level Compression level #CompressionLevel
   * @param strategy Compression strategy #CompressionStrategy
   * @param header Compression header #CompressionHeader
   * @param directBufferSize Size of the direct buffer to be used.
   */
  public CSSZlibCompressor(CompressionLevel level, CompressionStrategy strategy, 
                        CompressionHeader header, int directBufferSize) {
    this.level = level;
    this.strategy = strategy;
    this.windowBits = header;
    this.directBufferSize = directBufferSize;
    initZlib();
  }

  private void initZlib() {
    stream = init(this.level.compressionLevel(), 
                  this.strategy.compressionStrategy(), 
                  this.windowBits.windowBits());
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }


  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b== null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    uncompressedDirectBufOff = 0;
    setInputFromSavedData();
    
    // Reinitialize zlib's output direct buffer 
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }
  
  //copy enough data from userBuf to uncompressedDirectBuf
 public void setInputFromSavedData() {
    int len = Math.min(userBufLen, uncompressedDirectBuf.remaining());
    ((ByteBuffer)uncompressedDirectBuf).put(userBuf, userBufOff, len);
    userBufLen -= len;
    userBufOff += len;
    uncompressedDirectBufLen = uncompressedDirectBuf.position();
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    if (stream == 0 || b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
  }

  @Override
  public boolean needsInput() {
    // Consume remaining compressed data?
    if (compressedDirectBuf.remaining() > 0) {
      return false;
    }

    // Check if zlib has consumed all input
    // compress should be invoked if keepUncompressedBuf true
    if (keepUncompressedBuf && uncompressedDirectBufLen > 0)
      return false;
    
    if (uncompressedDirectBuf.remaining() > 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        // copy enough data from userBuf to uncompressedDirectBuf
        setInputFromSavedData();
        if (uncompressedDirectBuf.remaining() > 0) // uncompressedDirectBuf is not full
          return true;
        else 
          return false;
      }
    }
    
    return false;
  }
  
  @Override
  public void finish() {
    finish = true;
  }
  
  @Override
  public boolean finished() {
    // Check if 'zlib' says its 'finished' and
    // all compressed data has been consumed
    return (finished && compressedDirectBuf.remaining() == 0);
  }

  @Override
  public int compress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    int n = 0;
    
    // Check if there is compressed data
    n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)compressedDirectBuf).get(b, off, n);
      return n;
    }

    // Re-initialize the zlib's output direct buffer
    compressedDirectBuf.rewind();
    compressedDirectBuf.limit(directBufferSize);

    // Compress data
    n = deflateBytesDirect();
    compressedDirectBuf.limit(n);
    
    // Check if zlib consumed all input buffer
    // set keepUncompressedBuf properly
    if (uncompressedDirectBufLen <= 0) { // zlib consumed all input buffer
      keepUncompressedBuf = false;
      uncompressedDirectBuf.clear();
      uncompressedDirectBufOff = 0;
      uncompressedDirectBufLen = 0;
    } else { // zlib did not consume all input buffer
      keepUncompressedBuf = true;
    }
    
    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Returns the total number of compressed bytes output so far.
   *
   * @return the total (non-negative) number of compressed bytes output so far
   */
  @Override
  public long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  @Override
  public long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  @Override
  public void reset() {
    checkStream();
    reset(stream);
    finish = false;
    finished = false;
    uncompressedDirectBuf.rewind();
    uncompressedDirectBufOff = uncompressedDirectBufLen = 0;
    keepUncompressedBuf = false;
    compressedDirectBuf.limit(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;

  }
  
  @Override
  public void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  @Override
  public void reinit(org.apache.hadoop.conf.Configuration conf) {
    reset();
    end(stream);
    level = CompressionLevel.DEFAULT_COMPRESSION;
    strategy = CompressionStrategy.DEFAULT_STRATEGY;
    stream = init(level.compressionLevel(),
                  strategy.compressionStrategy(),
                  windowBits.windowBits());
  }

  
  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs();
  private native static long init(int level, int strategy, int windowBits);
  private native static void setDictionary(long strm, byte[] b, int off,
                                           int len);
  private native int deflateBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static void reset(long strm);
  private native static void end(long strm);

  public native static String getLibraryName();
  
}
