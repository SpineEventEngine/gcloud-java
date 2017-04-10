/*
 * Copyright 2017, TeamDev Ltd. All rights reserved.
 *
 * Redistribution and use in source and/or binary forms, with or without
 * modification, must retain the above copyright notice and the following
 * disclaimer.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.spine3.server.storage.datastore;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spine3.server.entity.EntityRecord;
import org.spine3.server.entity.storage.EntityRecordWithColumns;
import org.spine3.server.storage.RecordStorage;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Dmytro Dashenkov
 */
public class BigDataTester<I> {

    private static final int DEFAULT_BULK_SIZE = 500;

    private final RecordStorage<I> storage;

    private int bulkSize = DEFAULT_BULK_SIZE;
    private EntrySupplier<I> entrySupplier;
    private long writeMillisLimit;
    private long readMillisLimit;

    public BigDataTester(RecordStorage<I> target) {
        this.storage = target;
    }

    public BigDataTester setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
        return this;
    }

    public BigDataTester setEntrySupplier(EntrySupplier<I> entrySupplier) {
        this.entrySupplier = entrySupplier;
        return this;
    }

    public BigDataTester setWriteLimit(long writeMillisLimit) {
        this.writeMillisLimit = writeMillisLimit;
        return this;
    }

    public BigDataTester setReadLimit(long readMillisLimit) {
        this.readMillisLimit = readMillisLimit;
        return this;
    }

    public void testBigDataOperations() {
        final Map<I, EntityRecordWithColumns> records = new HashMap<>(bulkSize);
        for (int i = 0; i < bulkSize; i++) {
            records.put(entrySupplier.newId(), entrySupplier.newRecord());
        }

        final long writeStart = System.currentTimeMillis();
        storage.write(records);
        final long writeEnd = System.currentTimeMillis();

        final long writeTime = writeEnd - writeStart;
        if (writeTime > writeMillisLimit) {
            fail(format("Writing took too long. Expected %d millis but was %d millis.", writeMillisLimit, writeTime));
        }

        log().debug("Writing took {} millis.", writeTime);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail(Throwables.getStackTraceAsString(e));
        }

        final long readStart = System.currentTimeMillis();

        // Do not test data equality here, only the sizes and time
        final Map<I, EntityRecord> readRecords = storage.readAll();

        final long readEnd = System.currentTimeMillis();
        final long readTime = readEnd - readStart;

        if (readTime > readMillisLimit) {
            fail(format("Reading took too long. Expected %d millis but was %d millis.", readMillisLimit, readTime));
        }
        log().debug("Reading took {} millis.", readTime);

        assertEquals(records.size(), readRecords.size());
    }

    public interface EntrySupplier<I> {
        I newId();
        EntityRecordWithColumns newRecord();
    }

    private static Logger log() {
        return LogSingleton.INSTANCE.value;
    }

    private enum LogSingleton {
        INSTANCE;
        @SuppressWarnings("NonSerializableFieldInSerializableClass")
        private final Logger value = LoggerFactory.getLogger(BigDataTester.class);
    }
}
