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

import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.spine.core.Version;
import io.spine.string.Stringifiers;

import java.io.Serializable;
import java.util.Comparator;

import static com.google.cloud.Timestamp.ofTimeSecondsAndNanos;
import static io.spine.util.Exceptions.newIllegalArgumentException;

final class DefaultColumnTypeRegistry implements ColumnTypeRegistry {

    private static final ImmutableSortedMap<Class<?>, PersistenceStrategy<?>> defaultStrategies =
            defaultPolicies();

    @SuppressWarnings("unchecked") // Ensured by `defaultPolicies` declaration.
    @Override
    public <T> PersistenceStrategy<T> persistenceStrategyOf(Class<T> clazz) {
        PersistenceStrategy<?> strategy = defaultStrategies.get(clazz);
        if (strategy == null) {
            strategy = searchForSuperclassStrategy(clazz);
        }
        PersistenceStrategy<T> result = (PersistenceStrategy<T>) strategy;
        return result;
    }

    private static <T> PersistenceStrategy<?> searchForSuperclassStrategy(Class<T> clazz) {
        PersistenceStrategy<?> result =
                defaultPolicies().keySet()
                                 .stream()
                                 .filter(cls -> cls.isAssignableFrom(clazz))
                                 .map(defaultStrategies::get)
                                 .findFirst()
                                 .orElseThrow(() -> classNotFound(clazz));
        return result;
    }

    private static <T> IllegalArgumentException classNotFound(Class<T> clazz) {
        throw newIllegalArgumentException("The class %s is not found among registered types.",
                                          clazz.getCanonicalName());
    }

    private static ImmutableSortedMap<Class<?>, PersistenceStrategy<?>> defaultPolicies() {
        ImmutableSortedMap.Builder<Class<?>, PersistenceStrategy<?>> policies =
                ImmutableSortedMap.orderedBy(new SimplisticClassComparator());

        policies.put(String.class, new DefaultStringPersistenceStrategy());
        policies.put(Integer.class, new DefaultIntegerPersistenceStrategy());
        policies.put(Long.class, new DefaultLongPersistenceStrategy());
        policies.put(Double.class, new DefaultDoublePersistenceStrategy());
        policies.put(Float.class, new DefaultFloatPersistenceStrategy());
        policies.put(Boolean.class, new DefaultBooleanPersistenceStrategy());
        policies.put(Timestamp.class, new DefaultTimestampPersistenceStrategy());
        policies.put(Version.class, new DefaultVersionPersistenceStrategy());
        policies.put(Enum.class, new DefaultEnumPersistenceStrategy());
        policies.put(Message.class, new DefaultMessagePersistenceStrategy());

        return policies.build();
    }

    /**
     * A class comparator for the {@linkplain #defaultStrategies} map.
     *
     * <p>Compares classes in such a way so the subclasses go <b>before</b> their superclasses.
     *
     * <p>For the classes without "parent-child" relationship there is no predefined order of
     * storing.
     */
    private static class SimplisticClassComparator implements Comparator<Class<?>>, Serializable {

        private static final long serialVersionUID = 0L;

        @Override
        public int compare(Class<?> o1, Class<?> o2) {
            if (o1.equals(o2)) {
                return 0;
            }
            if (o1.isAssignableFrom(o2)) {
                return 1;
            }
            return -1;
        }
    }

    private static class DefaultStringPersistenceStrategy implements PersistenceStrategy<String> {

        @Override
        public Value<?> apply(String s) {
            return StringValue.of(s);
        }
    }

    private static class DefaultIntegerPersistenceStrategy implements PersistenceStrategy<Integer> {

        @Override
        public Value<?> apply(Integer integer) {
            return LongValue.of(integer);
        }
    }

    private static class DefaultLongPersistenceStrategy implements PersistenceStrategy<Long> {

        @Override
        public Value<?> apply(Long aLong) {
            return LongValue.of(aLong);
        }
    }

    private static class DefaultDoublePersistenceStrategy implements PersistenceStrategy<Double> {

        @Override
        public Value<?> apply(Double aDouble) {
            return DoubleValue.of(aDouble);
        }
    }

    private static class DefaultFloatPersistenceStrategy implements PersistenceStrategy<Float> {

        @Override
        public Value<?> apply(Float aFloat) {
            return DoubleValue.of(aFloat);
        }
    }

    private static class DefaultBooleanPersistenceStrategy
            implements PersistenceStrategy<Boolean> {

        @Override
        public Value<?> apply(Boolean aBoolean) {
            return BooleanValue.of(aBoolean);
        }
    }

    private static class DefaultTimestampPersistenceStrategy
            implements PersistenceStrategy<Timestamp> {

        @Override
        public Value<?> apply(Timestamp timestamp) {
            return TimestampValue.of(
                    ofTimeSecondsAndNanos(timestamp.getSeconds(), timestamp.getNanos())
            );
        }
    }

    private static class DefaultVersionPersistenceStrategy
            implements PersistenceStrategy<Version> {

        @Override
        public Value<?> apply(Version version) {
            int versionNumber = version.getNumber();
            return LongValue.of(versionNumber);
        }
    }

    private static class DefaultEnumPersistenceStrategy implements PersistenceStrategy<Enum<?>> {

        @Override
        public Value<?> apply(Enum<?> anEnum) {
            int ordinal = anEnum.ordinal();
            return LongValue.of(ordinal);
        }
    }

    private static class DefaultMessagePersistenceStrategy
            implements PersistenceStrategy<Message> {

        @Override
        public Value<?> apply(Message message) {
            String messageString = Stringifiers.toString(message);
            return StringValue.of(messageString);
        }
    }
}
