# rocks-kb — Project Notes

<!-- Migrated from CLAUDE.md by ws:init.
     This file is for hand-authored, project-specific information.
     Commit this file to git. -->

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
- Entity encoding uses NidCodec8: 8-bit pattern sequence + 24-bit element sequence (ike-issues#1138)
- Pattern entities stored under PATTERN_PATTERN_SEQUENCE (255, fixed); semantics stored under the pattern's element sequence; ordinary patterns are 1..254
- A database without a counter at 255 was written with the old 6-bit layout; `SequenceMap.open()` refuses it (`dev.ikm.tinkar.common.service.IncompatibleNidLayoutException`, non-retryable; Komet shows a dialog) — export with a 6-bit build, re-import
