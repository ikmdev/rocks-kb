package dev.ikm.ike.kb.validation;

import dev.ikm.tinkar.entity.export.ExportEntitiesToProtobufFile;
import dev.ikm.tinkar.entity.load.LoadEntitiesFromProtobufFile;
import dev.ikm.tinkar.fixtures.ForkedJvm;
import dev.ikm.tinkar.fixtures.StoreDigest;
import dev.ikm.tinkar.schema.TinkarMsg;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Changeset files, read and rewritten below the level of the loader, and the stages that
 * load them into a store: what the format tests share.
 *
 * <p>Records are handled as their bytes in the file — a length prefix and a message — and
 * never re-encoded, so a rewritten file keeps the format its writer wrote, whatever the
 * current schema would write.
 */
final class ChangeSets {

    static final String MANIFEST = "META-INF/MANIFEST.MF";
    static final String IDENTITY_INDEX = "META-INF/identities.pb";
    static final String FORMAT_VERSION = "Ike-Format-Version";

    /** The files a stage loads, in order, separated by {@link File#pathSeparator}. */
    static final String FILES = "files";
    /** How a {@link Load} stage imports: {@code default}, {@code one-pass} or {@code multi-pass}. */
    static final String IMPORT_MODE = "import.mode";
    static final String EXPORT_FILE = "export.file";
    static final String LOADED = "loaded.";

    private ChangeSets() {
    }

    // ---- Files

    static File resource(String name) {
        var url = ChangeSets.class.getClassLoader().getResource(name);
        assertTrue(url != null, "No test resource " + name);
        try {
            // toURI() decodes percent-encoding; getFile() breaks under paths with non-ASCII characters.
            return new File(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static Attributes manifest(File file) throws IOException {
        try (ZipFile zip = new ZipFile(file)) {
            ZipEntry entry = zip.getEntry(MANIFEST);
            assertTrue(entry != null, file + " has no manifest");
            return new Manifest(zip.getInputStream(entry)).getMainAttributes();
        }
    }

    static boolean hasEntry(File file, String name) throws IOException {
        try (ZipFile zip = new ZipFile(file)) {
            return zip.getEntry(name) != null;
        }
    }

    /** The bytes of a zip entry, or null if the file has no such entry. */
    static byte[] entry(File file, String name) throws IOException {
        try (ZipFile zip = new ZipFile(file)) {
            ZipEntry entry = zip.getEntry(name);
            return entry == null ? null : zip.getInputStream(entry).readAllBytes();
        }
    }

    /** The file's records, each as its bytes in the file: the length prefix and the message. */
    static List<byte[]> records(File file) throws IOException {
        List<byte[]> records = new ArrayList<>();
        try (ZipFile zip = new ZipFile(file)) {
            Enumeration<? extends ZipEntry> entries = zip.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (entry.getName().startsWith("META-INF/")) {
                    continue;
                }
                byte[] bytes = zip.getInputStream(entry).readAllBytes();
                int position = 0;
                while (position < bytes.length) {
                    int start = position;
                    int length = 0;
                    int shift = 0;
                    byte b;
                    do {
                        b = bytes[position++];
                        length |= (b & 0x7F) << shift;
                        shift += 7;
                    } while ((b & 0x80) != 0);
                    position += length;
                    byte[] record = new byte[position - start];
                    System.arraycopy(bytes, start, record, 0, record.length);
                    records.add(record);
                }
            }
        }
        return records;
    }

    /** The file's records, parsed with the current schema. */
    static List<TinkarMsg> parsedRecords(File file) throws IOException {
        List<TinkarMsg> parsed = new ArrayList<>();
        for (byte[] record : records(file)) {
            parsed.add(TinkarMsg.parseDelimitedFrom(new ByteArrayInputStream(record)));
        }
        return parsed;
    }

    /** The uncompressed size of the file's records. */
    static long recordBytes(File file) throws IOException {
        long total = 0;
        for (byte[] record : records(file)) {
            total += record.length;
        }
        return total;
    }

    /** A copy of the file with its records in reverse order and every META-INF entry unchanged. */
    static File reverseRecords(File source, File target) throws IOException {
        List<byte[]> records = records(source);
        Collections.reverse(records);
        return rewrite(source, target, records, null);
    }

    /**
     * A copy of the file with one main attribute of its manifest set to a new value. The
     * manifest is edited as text, every other line kept as the writer wrote it: a manifest
     * without a Manifest-Version, as changesets have, does not survive {@link Manifest#write}.
     */
    static File withManifestAttribute(File source, File target, String name, String value) throws IOException {
        String text = new String(entry(source, MANIFEST), StandardCharsets.UTF_8);
        String newline = text.contains("\r\n") ? "\r\n" : "\n";
        List<String> lines = new ArrayList<>(List.of(text.split(newline, -1)));
        // The main section ends at the first blank line; an attribute continues on lines that begin with a space.
        for (int i = 0; i < lines.size() && !lines.get(i).isEmpty(); i++) {
            if (lines.get(i).startsWith(name + ": ")) {
                lines.remove(i);
                while (i < lines.size() && lines.get(i).startsWith(" ")) {
                    lines.remove(i);
                }
                break;
            }
        }
        lines.addFirst(name + ": " + value);
        return rewrite(source, target, records(source), String.join(newline, lines).getBytes(StandardCharsets.UTF_8));
    }

    /** Writes the records as one entry, then the source's META-INF entries, the manifest replaced if given. */
    private static File rewrite(File source, File target, List<byte[]> records, byte[] manifest) throws IOException {
        try (ZipFile in = new ZipFile(source);
             ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(target.toPath()))) {
            out.putNextEntry(new ZipEntry("Entities"));
            for (byte[] record : records) {
                out.write(record);
            }
            out.closeEntry();
            Enumeration<? extends ZipEntry> entries = in.entries();
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                if (!entry.getName().startsWith("META-INF/")) {
                    continue;
                }
                out.putNextEntry(new ZipEntry(entry.getName()));
                if (manifest != null && entry.getName().equals(MANIFEST)) {
                    out.write(manifest);
                } else {
                    try (InputStream data = in.getInputStream(entry)) {
                        data.transferTo(out);
                    }
                }
                out.closeEntry();
            }
        }
        return target;
    }

    static dev.ikm.tinkar.schema.PublicId publicIdOf(TinkarMsg record) {
        return switch (record.getValueCase()) {
            case CONCEPT_CHRONOLOGY -> record.getConceptChronology().getPublicId();
            case SEMANTIC_CHRONOLOGY -> record.getSemanticChronology().getPublicId();
            case PATTERN_CHRONOLOGY -> record.getPatternChronology().getPublicId();
            case STAMP_CHRONOLOGY -> record.getStampChronology().getPublicId();
            case VALUE_NOT_SET -> throw new IllegalStateException("Tinkar message value not set");
        };
    }

    // ---- Stores

    /** A new, empty directory for one store lifetime of one test and provider. */
    static Path work(String test, Provider provider, String name) {
        Path work = Path.of("target", test, provider.name().toLowerCase(), name).toAbsolutePath();
        try {
            SnomedRoundTripIT.deleteTree(work);
            Files.createDirectories(work);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return work;
    }

    static Properties properties(Provider provider, Path store, File... files) {
        Properties in = new Properties();
        in.setProperty(StoreStage.PROVIDER, provider.name());
        in.setProperty(StoreStage.STORE, store.toString());
        List<String> paths = new ArrayList<>();
        for (File file : files) {
            paths.add(file.getAbsolutePath());
        }
        in.setProperty(FILES, String.join(File.pathSeparator, paths));
        return in;
    }

    static List<File> files(Properties in) {
        List<File> files = new ArrayList<>();
        for (String path : in.getProperty(FILES).split(File.pathSeparator)) {
            files.add(new File(path));
        }
        return files;
    }

    /** Loads the files, through the loader's default mode, into a new store of the provider in a JVM of its own. */
    static StoreDigest load(String test, Provider provider, String name, File... files) {
        return load(test, provider, name, "default", files);
    }

    /** Loads the files into a new store of the provider in a JVM of its own, and returns the store's digest. */
    static StoreDigest load(String test, Provider provider, String name, String importMode, File... files) {
        Properties in = properties(provider, work(test, provider, name).resolve("store"), files);
        in.setProperty(IMPORT_MODE, importMode);
        return StoreDigest.load(ForkedJvm.run(Load.class, in, Duration.ofMinutes(10)), LOADED);
    }

    /** Loads the files into a new store of the provider and exports the store; returns what the stage recorded. */
    static Properties loadAndExport(Provider provider, Path work, File export, File... files) {
        Properties in = properties(provider, work.resolve("store"), files);
        in.setProperty(EXPORT_FILE, export.getAbsolutePath());
        return ForkedJvm.run(LoadAndExport.class, in, Duration.ofMinutes(10));
    }

    /** A new store and the files loaded into it, in order, in the import mode the properties name. */
    static class Load extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            long start = System.currentTimeMillis();
            for (File file : files(in)) {
                LoadEntitiesFromProtobufFile loader = switch (in.getProperty(IMPORT_MODE, "default")) {
                    case "one-pass" -> new LoadEntitiesFromProtobufFile(file, false);
                    case "multi-pass" -> new LoadEntitiesFromProtobufFile(file, true);
                    default -> new LoadEntitiesFromProtobufFile(file);
                };
                loader.compute();
            }
            out.setProperty("load.millis", Long.toString(System.currentTimeMillis() - start));
            StoreDigest.ofOpenStore().store(out, LOADED);
        }
    }

    /** A new store, the files loaded into it, and the store exported in the current format. */
    static class LoadAndExport extends StoreStage {
        @Override
        void work(Properties in, Properties out) {
            new Load().work(in, out);
            File exportFile = new File(in.getProperty(EXPORT_FILE));
            long start = System.currentTimeMillis();
            long exported = new ExportEntitiesToProtobufFile(exportFile).compute().getTotalCount();
            out.setProperty("export.millis", Long.toString(System.currentTimeMillis() - start));
            out.setProperty("export.count", Long.toString(exported));
        }
    }
}
