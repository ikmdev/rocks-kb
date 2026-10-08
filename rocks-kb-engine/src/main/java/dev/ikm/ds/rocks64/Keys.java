package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.id.Nid;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * The byte forms of a 64-bit store's keys. An entity's key is its nid, eight bytes big-endian;
 * a UUID's is its sixteen bytes; a reference's is the referenced nid followed by the referencing
 * nid, so the references to one entity are a prefix range, and those from one pattern a longer
 * one, since a nid's upper four bytes are its pattern sequence.
 */
final class Keys {

    private Keys() {
    }

    static byte[] of(long nid) {
        return ByteBuffer.allocate(8).putLong(nid).array();
    }

    static long nid(byte[] key) {
        return ByteBuffer.wrap(key).getLong();
    }

    static byte[] of(UUID uuid) {
        return ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
    }

    static UUID uuid(byte[] key) {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    static byte[] reference(long referencedNid, long referencingNid) {
        return ByteBuffer.allocate(16).putLong(referencedNid).putLong(referencingNid).array();
    }

    /** The prefix of every reference to an entity. */
    static byte[] referencesTo(long referencedNid) {
        return of(referencedNid);
    }

    /** The prefix of every reference to an entity from the semantics of one pattern. */
    static byte[] referencesTo(long referencedNid, int patternSequence) {
        return ByteBuffer.allocate(12).putLong(referencedNid).putInt(patternSequence).array();
    }

    static long referencingNid(byte[] referenceKey) {
        return ByteBuffer.wrap(referenceKey, 8, 8).getLong();
    }

    static boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    /** The first key of a pattern. */
    static byte[] firstOf(int patternSequence) {
        return of(Nid.compose64(patternSequence, Counters.FIRST_ELEMENT));
    }

    /** The key just past every key of a pattern: the first key of the next pattern. */
    static byte[] endOf(int patternSequence) {
        return patternSequence >= Nid.MAX_SEQUENCE_64
                ? new byte[]{(byte) 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}
                : of(Nid.compose64(patternSequence + 1, Counters.FIRST_ELEMENT));
    }
}
