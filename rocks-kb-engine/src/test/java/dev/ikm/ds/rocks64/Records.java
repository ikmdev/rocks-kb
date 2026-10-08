package dev.ikm.ds.rocks64;

import dev.ikm.tinkar.common.service.EntityRecordFormat2;
import io.activej.bytebuf.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/** Synthetic format 2 records for the engine's tests: a chronology with UUIDs, and one version per stamp. */
final class Records {

    static final byte CONCEPT = 1;
    static final byte CONCEPT_VERSION = 4;

    private Records() {
    }

    static byte[] chronology(byte token, long nid, UUID... uuids) {
        ByteBuf buf = ByteBuf.wrapForWriting(new byte[256]);
        buf.writeByte(token);
        long[] additional = new long[(uuids.length - 1) * 2];
        for (int i = 1; i < uuids.length; i++) {
            additional[2 * (i - 1)] = uuids[i].getMostSignificantBits();
            additional[2 * (i - 1) + 1] = uuids[i].getLeastSignificantBits();
        }
        EntityRecordFormat2.writeIdentity(buf, nid, uuids[0].getMostSignificantBits(), uuids[0].getLeastSignificantBits(), additional);
        buf.writeInt(0xCAFE);
        return Arrays.copyOf(buf.array(), buf.tail());
    }

    static byte[] version(byte token, long stampNid, int payload) {
        ByteBuf buf = ByteBuf.wrapForWriting(new byte[64]);
        buf.writeByte(token);
        EntityRecordFormat2.writeNid(buf, stampNid);
        buf.writeInt(payload);
        return Arrays.copyOf(buf.array(), buf.tail());
    }

    /** A concept record under a nid with the given UUIDs, one version per stamp nid. */
    static byte[] concept(long nid, UUID[] uuids, long... stampNids) {
        List<byte[]> versions = new ArrayList<>();
        for (long stamp : stampNids) {
            versions.add(version(CONCEPT_VERSION, stamp, (int) stamp));
        }
        return EntityRecordFormat2.assemble(chronology(CONCEPT, nid, uuids), versions);
    }

    static byte[] concept(long nid, long... stampNids) {
        return concept(nid, new UUID[]{UUID.randomUUID()}, stampNids);
    }

    static int versionCount(byte[] record) {
        return EntityRecordFormat2.parts(record).versions().size();
    }
}
