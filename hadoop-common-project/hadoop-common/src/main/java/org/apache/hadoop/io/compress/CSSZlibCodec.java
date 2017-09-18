/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.compress;

import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.compress.zlib.*;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CSSZlibCodec extends GzipCodec {

	private static final Log LOG = LogFactory.getLog(CSSZlibCodec.class);

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
        return CompressionCodec.Util.
                createOutputStreamWithCodecPool(this, conf, out);
	}

	@Override
	public Compressor createCompressor() {
		return new CSSZlibCompressor(); 
	}

	@Override
	public Decompressor createDecompressor() {
		return new CSSZlibDecompressor(); 
	}

	@Override
	public Class<? extends Compressor> getCompressorType() {
		return CSSZlibCompressor.class; 
	}
	
	@Override
	public Class<? extends Decompressor> getDecompressorType() {
		return CSSZlibDecompressor.class; 
	}
	
	@Override
	public String getDefaultExtension() {
		return ".csszlib";
	}

}
