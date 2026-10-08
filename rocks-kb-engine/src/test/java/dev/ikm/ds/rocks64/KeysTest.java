package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeysTest {

    @Test
    void aNidKeyIsItsEightBytesBigEndian() {
        long nid = Nid.compose64(5, 7);
        byte[] key = Keys.of(nid);
        assertEquals(8, key.length);
        assertArrayEquals(new byte[]{0, 0, 0, 5, 0, 0, 0, 7}, key);
        assertEquals(nid, Keys.nid(key));
    }

    @Test
    void aUuidKeyRoundTrips() {
        UUID uuid = UUID.randomUUID();
        assertEquals(16, Keys.of(uuid).length);
        assertEquals(uuid, Keys.uuid(Keys.of(uuid)));
    }

    @Test
    void aReferenceKeyIsTheReferencedThenTheReferencingNid() {
        long referenced = Nid.compose64(2, 9);
        long referencing = Nid.compose64(4, 3);
        byte[] key = Keys.reference(referenced, referencing);
        assertEquals(16, key.length);
        assertTrue(Keys.startsWith(key, Keys.referencesTo(referenced)));
        assertTrue(Keys.startsWith(key, Keys.referencesTo(referenced, 4)));
        assertEquals(12, Keys.referencesTo(referenced, 4).length);
        assertEquals(referencing, Keys.referencingNid(key));
    }

    @Test
    void aPatternsKeysLieBetweenItsFirstKeyAndItsEnd() {
        int pattern = 6;
        byte[] first = Keys.firstOf(pattern);
        byte[] end = Keys.endOf(pattern);
        byte[] last = Keys.of(Nid.compose64(pattern, Nid.MAX_SEQUENCE_64));
        assertEquals(0, Arrays.compareUnsigned(first, Keys.of(Nid.compose64(pattern, 1))));
        assertTrue(Arrays.compareUnsigned(first, last) < 0);
        assertTrue(Arrays.compareUnsigned(last, end) < 0);
        assertEquals(0, Arrays.compareUnsigned(end, Keys.firstOf(pattern + 1)));
    }

    @Test
    void theLastPatternsEndIsBeyondEveryKey() {
        byte[] end = Keys.endOf(Nid.MAX_SEQUENCE_64);
        assertTrue(Arrays.compareUnsigned(Keys.of(Nid.compose64(Nid.MAX_SEQUENCE_64, Nid.MAX_SEQUENCE_64)), end) < 0);
    }
}
