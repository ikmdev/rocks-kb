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
- Nid layout is per database (ike-issues#1138): new databases use NidCodec8 (8-bit pattern + 24-bit element, up to 255 patterns); a database written with NidCodec6 (6-bit + 26-bit, 63 patterns) is opened in 6-bit mode
- `SequenceMap.open()` detects the layout (counter at 255 → 8-bit; none → 6-bit; empty → new, 8-bit) and activates the process-wide `NidLayout`; everything encodes/decodes nids through `NidLayout.active()`
- Pattern entities are keyed under `SequenceMap.patternPatternSequence()` (255 or 63); semantics under the pattern's element sequence; ordinary patterns are 1..254 (8-bit) or 1..62 (6-bit)
- Komet warns after opening a 6-bit database: open in 6-bit mode (read-write, title shows "6-bit database") or migrate (export, relaunch into "New Rocks KB" from the export)
