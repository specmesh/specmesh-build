/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.specmesh.kafka.util;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.hamcrest.Matcher;

/** Hamcrest async assert with timeout. */
public final class AssertEventually {

    /** A filter used to control how exceptions thrown by the check are handler. */
    @FunctionalInterface
    public interface ExceptionFilter {

        /**
         * Called on exception.
         *
         * @param e the exception
         */
        void accept(RuntimeException e);
    }

    /** An exception filter that causing any exceptions to be thrown out of the assertion */
    public static final ExceptionFilter FailOnException =
            e -> {
                throw e;
            };

    /**
     * An exception filter that swallows any exceptions, allowing the assertion to continue checking
     */
    public static final ExceptionFilter RetryOnException = e -> {};

    /**
     * Hamcrest style assertion, taking a value supplier rather than a value.
     *
     * <p>The supplier is polled periodically until either the matcher matches or a timeout is
     * reached.
     *
     * @param actualSupplier the actual value supplied
     * @param expected the expected matcher
     * @param <T> the type of the value being matched
     * @return the value that matched the matcher
     * @throws AssertionError on failure to match within the timeout.
     * @throws RuntimeException if the supplier throws
     */
    public static <T> T assertThatEventually(
            final Supplier<? extends T> actualSupplier, final Matcher<? super T> expected) {
        return assertThatEventually(actualSupplier, expected, withSettings());
    }

    /**
     * Hamcrest style assertion, taking a value supplier rather than a value.
     *
     * <p>The supplier is polled periodically until either the matcher matches or a timeout is
     * reached.
     *
     * <p>The timeout, check period and other functionality is configurable via the supplied {@link
     * Settings},
     *
     * @param actualSupplier the actual value supplied
     * @param expected the expected matcher
     * @param settings settings to control the behaviour
     * @param <T> the type of the value being matched
     * @return the value that matched the matcher
     * @throws AssertionError on failure to match within the timeout.
     * @throws RuntimeException if the supplier throws
     */
    @SuppressWarnings("BusyWait")
    public static <T> T assertThatEventually(
            final Supplier<? extends T> actualSupplier,
            final Matcher<? super T> expected,
            final Settings settings) {
        try {
            final Instant end = Instant.now().plus(settings.timeout);

            Duration period = settings.initialPeriod;
            while (Instant.now().isBefore(end)) {
                T actual = null;
                boolean acquired = false;

                try {
                    actual = actualSupplier.get();
                    acquired = true;
                } catch (final RuntimeException e) {
                    settings.exceptionFilter.accept(e);
                }

                if (acquired && expected.matches(actual)) {
                    return actual;
                }

                Thread.sleep(period.toMillis());

                period = increasePeriod(settings, period);
            }

            final T actual = actualSupplier.get();
            assertThat(settings.message.get(), actual, expected);
            return actual;
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Factory method for settings builder
     *
     * @return settings builder
     */
    public static Settings withSettings() {
        return new Settings();
    }

    /** Settings to control the assertion. */
    public static final class Settings {

        private Supplier<String> message = () -> "";
        private ExceptionFilter exceptionFilter = FailOnException;
        private Duration timeout = Duration.ofSeconds(30);
        private Duration initialPeriod = Duration.ofMillis(1);
        private Duration maxPeriod = Duration.ofSeconds(1);

        private Settings() {}

        /**
         * Set a custom message used on failure
         *
         * @param message the custom message
         * @return self.
         */
        public Settings withMessage(final String message) {
            requireNonNull(message, "message");
            this.message = () -> message;
            return this;
        }

        /**
         * Set a custom message used on failure
         *
         * @param message the custom message supplier. Only called on failure
         * @return self.
         */
        public Settings withMessage(final Supplier<String> message) {
            this.message = requireNonNull(message, "message");
            return this;
        }

        /**
         * Set a custom the exception filter
         *
         * @param filter the custom filter
         * @return self.
         */
        public Settings withExceptionFilter(final ExceptionFilter filter) {
            this.exceptionFilter = requireNonNull(filter, "filter");
            return this;
        }

        /**
         * Customise how long to wait before the assertion fails.
         *
         * @param timeout the custom timeout.
         * @return self.
         */
        public Settings withTimeout(final Duration timeout) {
            this.timeout = requireNonNull(timeout, "timeout");
            return this;
        }

        /**
         * The initial duration to wait before testing
         *
         * <p>After this initial period}, if the check fails, the duration to wait before trying
         * again will double, and keep doubling on subsequent failures, up to a {@link
         * #withMaxPeriod maximum}.
         *
         * @param period initial duration
         * @return self.
         */
        public Settings withInitialPeriod(final Duration period) {
            if (period.isZero() || period.isNegative()) {
                throw new IllegalArgumentException("period must be positive");
            }
            this.initialPeriod = requireNonNull(period, "period");
            return this;
        }

        /**
         * The maximum duration between attempts.
         *
         * <p>After the {@link #withInitialPeriod initial period}, if the check fails, the duration
         * to wait before trying again will double, and keep doubling on subsequent failures, up to
         * this maximum.
         *
         * @param period maximum duration between attempts
         * @return self.
         */
        public Settings withMaxPeriod(final Duration period) {
            this.maxPeriod = requireNonNull(period, "period");
            return this;
        }
    }

    private AssertEventually() {}

    private static Duration increasePeriod(final Settings settings, final Duration currentPeriod) {
        final Duration doubled = currentPeriod.multipliedBy(2);
        return doubled.compareTo(settings.maxPeriod) < 0 ? doubled : settings.maxPeriod;
    }
}
