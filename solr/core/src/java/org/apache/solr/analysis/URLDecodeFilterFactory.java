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
package org.apache.solr.analysis;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/** Filter the URLDecodes URL-encoded input */
public class URLDecodeFilterFactory extends TokenFilterFactory {

  public static final String NAME = "urlDecode";

  /** Creates a new ReversedWildcardFilterFactory */
  public URLDecodeFilterFactory(Map<String, String> args) {
    super(args);
  }

  /** Default ctor for compatibility with SPI */
  public URLDecodeFilterFactory() {
    throw defaultCtorException();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new TokenFilter(input) {
      private final CharTermAttribute termAtt = input.getAttribute(CharTermAttribute.class);

      @Override
      public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
          return false;
        }
        String decoded = URLDecoder.decode(termAtt.toString(), StandardCharsets.UTF_8);
        termAtt.setEmpty();
        termAtt.append(decoded);
        return true;
      }
    };
  }
}
