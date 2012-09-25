/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.mongodb.hadoop.util;

import com.mongodb.*;
import org.apache.commons.logging.*;
import org.bson.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class BSONLoader implements Iterable<BSONObject>, Iterator<BSONObject> {

    LazyDBDecoder lazyDecoder = new LazyDBDecoder();
    boolean hasNext = true;
    private final InputStream inputStream;
    private static final Log log = LogFactory.getLog( BSONLoader.class );
    private AtomicBoolean hasMore = new AtomicBoolean( true );
    LazyDBObject currentDBObj = null;

    public BSONLoader(final InputStream input) {
        inputStream = input;
        try {
            currentDBObj = (LazyDBObject)lazyDecoder.decode( inputStream, (DBCollection) null );
        } catch (Exception exp) {
            log.debug( "Error reading record, may be EOF",exp );
            hasMore.set( false );
        }
    }

    public Iterator<BSONObject> iterator(){
        return this;
    }

    public boolean hasNext(){
         return hasMore.get();
    }


    public synchronized BSONObject next(){
        try {
            LazyDBObject nextDBObj = (LazyDBObject)lazyDecoder.decode( inputStream, (DBCollection) null );
            LazyDBObject tempDBObj = currentDBObj;
            currentDBObj = nextDBObj;
            return (BSONObject) tempDBObj;
        } catch ( Exception e ) {
            /* If we can't read another length it's not an error, just return quietly. */
            log.debug( "Error reading record, may be EOF",e );
            hasMore.set( false );
            try {
                inputStream.close();
            } catch ( IOException e1 ) {
                log.debug( "Error closing input stream",e1 );
            }
        }
        return currentDBObj;
    }

    public void remove(){
        throw new UnsupportedOperationException();
    }

}
