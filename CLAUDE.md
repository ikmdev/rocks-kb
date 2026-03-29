# Rocks KB

RocksDB-based knowledge base storage engine for Tinkar entities.

## Build Standards

Files in `.claude/standards/` are build artifacts unpacked from `ike-build-standards`. DO NOT edit or commit them. See the workspace root CLAUDE.md for details.

## Build

```bash
mvn clean verify -DskipTests -T4
```

## Key Facts

- GroupId: `dev.ikm.ike`
- Uses `--enable-preview` (Java 25) — set via `maven.compiler.enablePreview`
- BOM: imports `dev.ikm.ike:ike-bom`
- Entity encoding uses NidCodec6: 6-bit pattern sequence + 26-bit element index
- Pattern entities stored under PATTERN_PATTERN_SEQUENCE (63); semantics stored under the pattern's element sequence
