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
 * A test utility that checks that the data read/write operations perform well (i.e. execute within
 * the specified time
 * limit) upon a big amount of records.
 *
 * <p>The default count of records is 500. Use {@link #setBulkSize(int)} to modify this number.
 *
 * <p>Note: the operations expected to be executed upon the whole bulk of records, not each record
 * separately.
 *
 * <p>The test suits based on this utility are not expected to check if the read records match
 * those that were written or not. Instead they should just check the time consumed by
 * the operations and the consistency of the data (i.e. count of the records written and read).
 *
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

    /**
     * Preforms the actual check.
     *
     * <p>The execution flow is as follows:
     * <ol>
     *     <li>1. Produce the records with the given {@link EntrySupplier}
     *     <li>2. Measure the time of the {@linkplain RecordStorage#write(Map) bulk write}
     *     <li>3. Fail if the time is over the specified limit
     *     <li>4. Wait 1 second to ensure the Datastore has established the data consistency
     *     <li>5. Measure the time of the {@linkplain RecordStorage#readAll() bulk read}
     *     <li>6. Fail if the time is over the specified limit
     *     <li>7. Check the count of the records written and read is equal
     * </ol>
     *
     * <p>This method performs {@code debug} logging of the measure results. To see the log, run
     * the tests with {@code debug} logging.
     */
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

        assertEquals("Unexpected records count read.", records.size(), readRecords.size());
    }

    /**
     * A supplier of the records to write into the Datastore for the check.
     *
     * <p>It's recommended to provide generic non-empty records to make the check closer to a real-life cases.
     *
     * @param <I> the type of the record ID
     */
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
