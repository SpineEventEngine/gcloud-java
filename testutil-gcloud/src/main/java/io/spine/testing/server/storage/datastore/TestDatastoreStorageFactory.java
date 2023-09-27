/*
 * Copyright 2023, TeamDev. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

package io.spine.testing.server.storage.datastore;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.flogger.FluentLogger;
import io.spine.annotation.Internal;
import io.spine.server.entity.storage.ColumnMapping;
import io.spine.server.storage.datastore.DatastoreStorageFactory;
import io.spine.server.storage.datastore.DatastoreWrapper;
import io.spine.server.storage.datastore.DsColumnMapping;

import java.util.Collection;
import java.util.HashSet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A test implementation of the {@link DatastoreStorageFactory}.
 *
 * <p>Wraps the datastore with an instance of {@link TestDatastoreWrapper} and provides additional
 * clean up {@linkplain #tearDown() methods}.
 */
public class TestDatastoreStorageFactory extends DatastoreStorageFactory {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Collection<DatastoreWrapper> allCreatedWrappers = new HashSet<>();

    protected TestDatastoreStorageFactory(Datastore datastore) {
        this(datastore, new DsColumnMapping());
    }

    protected TestDatastoreStorageFactory(Datastore datastore, ColumnMapping<Value<?>> mapping) {
        super(DatastoreStorageFactory
                      .newBuilder()
                      .setDatastore(datastore)
                      .setColumnMapping(mapping)
        );
    }

    /**
     * Creates a new instance which works with a local Datastore emulator.
     *
     * <p>A shortcut for {@code basedOn(TestDatastores.local())}.
     */
    public static TestDatastoreStorageFactory local() {
        return basedOn(TestDatastores.local());
    }

    /**
     * Creates a new instance which works with a local Datastore emulator.
     *
     * <p>A shortcut for {@code basedOn(TestDatastores.local())}.
     */
    public static TestDatastoreStorageFactory local(ColumnMapping<Value<?>> mapping) {
        return basedOn(TestDatastores.local(), mapping);
    }

    /**
     * Creates a new factory instance which wraps the given Datastore.
     */
    public static TestDatastoreStorageFactory basedOn(Datastore datastore) {
        checkNotNull(datastore);
        return new TestDatastoreStorageFactory(datastore);
    }

    /**
     * Creates a new factory instance which wraps the given Datastore.
     */
    private static TestDatastoreStorageFactory
    basedOn(Datastore datastore, ColumnMapping<Value<?>> mapping) {
        checkNotNull(datastore);
        return new TestDatastoreStorageFactory(datastore, mapping);
    }

    @Internal
    @Override
    public DatastoreWrapper createDatastoreWrapper(boolean multitenant) {
        TestDatastoreWrapper wrapper = TestDatastoreWrapper.wrap(datastore(), false);
        allCreatedWrappers.add(wrapper);
        return wrapper;
    }

    @Override
    protected Iterable<DatastoreWrapper> wrappers() {
        return ImmutableSet.copyOf(allCreatedWrappers);
    }

    /**
     * Performs operations on setting up the local datastore.
     *
     * <p>By default is a NO-OP, but can be overridden.
     */
    public void setUp() {
        // NO-OP. See doc.
    }

    /**
     * Clears all data in the local Datastore.
     *
     * <p>May be effectively the same as {@link #clear()}.
     *
     * <p><b>NOTE</b>: does not stop the server but just deletes all records.
     *
     * <p>Equivalent to dropping all tables in an SQL-based storage.
     */
    public void tearDown() {
        clear();
    }

    /**
     * Clears all data in the Datastore.
     *
     * @see #tearDown()
     */
    public void clear() {
        Iterable<DatastoreWrapper> wrappers = wrappers();
        for (DatastoreWrapper wrapper : wrappers) {
            TestDatastoreWrapper datastore = (TestDatastoreWrapper) wrapper;
            try {
                datastore.dropAllTables();
            } catch (Throwable e) {
                logger.atSevere()
                      .withCause(e)
                      .log("Unable to drop tables in Datastore `%s`.", datastore);
                throw new IllegalStateException(e);
            }
        }
    }
}
