package dev.ikm.ike.kb.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/** A count a stage carries back to its test, with the first few examples of what it counted. */
final class Count {

    /** At most this many examples are carried back. */
    private static final int EXAMPLES = 20;

    private final AtomicLong count = new AtomicLong();
    private final List<String> examples = new ArrayList<>();

    void add() {
        count.incrementAndGet();
    }

    void add(String example) {
        count.incrementAndGet();
        synchronized (examples) {
            if (examples.size() < EXAMPLES) {
                examples.add(example);
            }
        }
    }

    long get() {
        return count.get();
    }

    void store(Properties out, String key) {
        out.setProperty(key, Long.toString(count.get()));
        synchronized (examples) {
            if (!examples.isEmpty()) {
                out.setProperty(key + ".examples", String.join("; ", examples));
            }
        }
    }
}
