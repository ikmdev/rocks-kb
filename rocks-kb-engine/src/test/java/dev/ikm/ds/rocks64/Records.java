package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.service.EntityRecordFormat3;
import io.activej.bytebuf.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Synthetic format 3 records for the engine's tests: a chronology with UUIDs, and one version per stamp. */
final class Records {

    static final byte CONCEPT = 1;
    static final byte CONCEPT_VERSION = 4;

    private Records() {
    }

    static byte[] chronology(byte token, long nid, UUID... uuids) {
        // The nid gives the kind in format 3; the token is kept for the callers' readability.
        ByteBuf buf = ByteBuf.wrapForWriting(new byte[256]);
        long[] additional = new long[(uuids.length - 1) * 2];
        for (int i = 1; i < uuids.length; i++) {
            additional[2 * (i - 1)] = uuids[i].getMostSignificantBits();
            additional[2 * (i - 1) + 1] = uuids[i].getLeastSignificantBits();
        }
        EntityRecordFormat3.writeIdentity(buf, nid, uuids[0].getMostSignificantBits(), uuids[0].getLeastSignificantBits(), additional);
        buf.writeInt(0xCAFE);
        return Arrays.copyOf(buf.array(), buf.tail());
    }

    static byte[] version(byte token, long stampNid, int payload) {
        ByteBuf buf = ByteBuf.wrapForWriting(new byte[64]);
        EntityRecordFormat3.writeStampNid(buf, stampNid);
        buf.writeInt(payload);
        return Arrays.copyOf(buf.array(), buf.tail());
    }

    /** A concept record under a nid with the given UUIDs, one version per stamp nid. */
    static byte[] concept(long nid, UUID[] uuids, long... stampNids) {
        List<byte[]> versions = new ArrayList<>();
        for (long stamp : stampNids) {
            versions.add(version(CONCEPT_VERSION, stamp, (int) stamp));
        }
        return EntityRecordFormat3.assemble(chronology(CONCEPT, nid, uuids), versions);
    }

    static byte[] concept(long nid, long... stampNids) {
        return concept(nid, new UUID[]{UUID.randomUUID()}, stampNids);
    }

    static int versionCount(byte[] record) {
        return EntityRecordFormat3.parts(record).versions().size();
    }
}
