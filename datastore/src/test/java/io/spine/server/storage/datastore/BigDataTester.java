/*
 * Copyright 2019, TeamDev. All rights reserved.
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

package io.spine.server.storage.datastore;

import com.google.common.base.Throwables;
import io.spine.logging.Logging;
import io.spine.server.entity.EntityRecord;
import io.spine.server.entity.storage.EntityRecordWithColumns;
import io.spine.server.storage.RecordStorage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.size;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A test utility that checks that the data read/write operations perform well (i.e. execute within
 * the specified time limit) upon a big amount of records.
 *
 * <p>The default count of records is {@link #DEFAULT_BULK_SIZE}.
 * Use {@link Builder#setBulkSize(int)} to modify this number.
 *
 * <p>Note: the operations expected to be executed upon the whole bulk of records, not each record
 * separately.
 *
 * <p>The test suits based on this utility are not expected to check if the read records match
 * those that were written or not. Instead they should just check the time consumed by
 * the operations and the consistency of the data (i.e. count of the records written and read).
 *
 * @param <I>
 *         the type of the ID in the tested {@linkplain RecordStorage}
 */
public class BigDataTester<I> implements Logging {

    private static final int DEFAULT_BULK_SIZE = 500;

    private final int bulkSize;
    private final EntryFactory<I> entryFactory;
    private final long writeMillisLimit;
    private final long readMillisLimit;

    private BigDataTester(Builder<I> builder) {
        this.bulkSize = builder.bulkSize;
        this.entryFactory = builder.entryFactory;
        this.writeMillisLimit = builder.writeMillisLimit;
        this.readMillisLimit = builder.readMillisLimit;
    }

    public static <I> Builder<I> newBuilder() {
        return new Builder<>();
    }

    /**
     * Performs the actual check.
     *
     * <p>The execution flow is as follows:
     * <ol>
     * <li>1. Produce the records with the given {@link EntryFactory}
     * <li>2. Measure the time of the {@linkplain RecordStorage#write(Map) bulk write}
     * <li>3. Fail if the time is over the specified limit
     * <li>4. Wait 1 second to ensure the Datastore has established the data consistency
     * <li>5. Measure the time of the {@linkplain RecordStorage#readAll() bulk read}
     * <li>6. Fail if the time is over the specified limit
     * <li>7. Check the count of the records written and read is equal
     * </ol>
     *
     * <p>This method performs {@code debug} logging of the measure results. To see the log, run
     * the tests with {@code debug} logging.
     */
    public void testBigDataOperations(RecordStorage<I> storage) {
        checkNotNull(storage);
        Map<I, EntityRecordWithColumns> records = new HashMap<>(bulkSize);
        for (int i = 0; i < bulkSize; i++) {
            records.put(entryFactory.newId(), entryFactory.newRecord());
        }

        long writeStart = System.currentTimeMillis();
        storage.write(records);
        long writeEnd = System.currentTimeMillis();

        long writeTime = writeEnd - writeStart;
        if (writeTime > writeMillisLimit) {
            fail(format("Writing took too long. Expected %d millis but was %d millis.",
                        writeMillisLimit,
                        writeTime));
        }

        log().debug("Writing took {} millis.", writeTime);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail(Throwables.getStackTraceAsString(e));
        }

        long readStart = System.currentTimeMillis();

        // Do not test data equality here, only the sizes and time
        Iterator<EntityRecord> readRecords = storage.readAll();

        long readEnd = System.currentTimeMillis();
        long readTime = readEnd - readStart;

        if (readTime > readMillisLimit) {
            fail(format("Reading took too long. Expected %d millis but was %d millis.",
                        readMillisLimit,
                        readTime));
        }
        _debug().log("Reading took %s millis.", readTime);

        assertEquals(records.size(), size(readRecords), "Unexpected records count read.");
    }

    /**
     * A supplier of the records to write into the Datastore for the check.
     *
     * <p>It's recommended to provide generic non-empty records to make the check closer to a
     * real-life cases.
     *
     * @param <I>
     *         the type of the record ID
     */
    public interface EntryFactory<I> {

        I newId();

        EntityRecordWithColumns newRecord();
    }

    /**
     * A builder for the {@code BigDataTester}.
     *
     * @param <I>
     *         the target type of the ID in the tested {@linkplain RecordStorage}
     */
    public static class Builder<I> {

        private int bulkSize;
        private EntryFactory<I> entryFactory;
        private long writeMillisLimit;
        private long readMillisLimit;

        /**
         * Prevents direct instantiation.
         */
        private Builder() {
        }

        /**
         * Assigns the site of the test bulk.
         *
         * @param bulkSize
         *         the size of the test bulk; the {@link EntryFactory} methods will be
         *         called exactly this number of times; the default value is
         *         {@link #DEFAULT_BULK_SIZE}
         */
        @SuppressWarnings("unused")
        public Builder<I> setBulkSize(int bulkSize) {
            checkArgument(bulkSize > 0,
                          "The records bulk size should be greater then 0.");
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Assigns an {@link EntryFactory} which generates the test data.
         */
        public Builder<I> setEntryFactory(EntryFactory<I> entryFactory) {
            this.entryFactory = entryFactory;
            return this;
        }

        /**
         * Assigns the the max time in milliseconds which is allowed for the write
         * operation to execute for.
         */
        public Builder<I> setWriteLimit(long writeMillisLimit) {
            checkArgument(writeMillisLimit > 0,
                          "The write time limit should be greater then 0.");
            this.writeMillisLimit = writeMillisLimit;
            return this;
        }

        /**
         * Assigns the max time in milliseconds which is allowed for the read
         * operation to execute for.
         */
        public Builder<I> setReadLimit(long readMillisLimit) {
            checkArgument(readMillisLimit > 0,
                          "The read time limit should be greater then 0.");
            this.readMillisLimit = readMillisLimit;
            return this;
        }

        /**
         * Creates new instance of the {@code BigDataTester}.
         */
        public BigDataTester<I> build() {
            checkNotNull(entryFactory);
            if (bulkSize == 0) {
                bulkSize = DEFAULT_BULK_SIZE;
            }
            checkArgument(writeMillisLimit != 0, "Write time limit should be set.");
            checkArgument(readMillisLimit != 0, "Read time limit should be set.");
            BigDataTester<I> tester = new BigDataTester<>(this);
            return tester;
        }
    }
}
