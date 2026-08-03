package redis

import (
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
)

// buildPreflight classifies a Redis source against a Redis target (RM2). Redis has
// no relational type-compat / FK axis; the only §1.7 blocks are:
//
//   - the RDB-version DIRECTIONAL gate: RESTORE rejects a payload whose RDB version
//     exceeds the target's, so a newer-source -> older-target pair fails loud. We
//     approximate max-RDB-version by server version (coarse but safe) and BLOCK
//     when the target is older than the source.
//   - the MODULE gate: a module loaded on the source but absent on the target means
//     a module-typed value cannot be RESTOREd faithfully -> BLOCK. (Coarse:
//     presence-based, so it may block on an unused module or miss a module *version*
//     mismatch — documented, redis-plan m6.)
//
// Every unit is a single-member acyclic component (no FK graph).
func buildPreflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) *engine.PreflightReport {
	r := &engine.PreflightReport{
		Sync:          syncName,
		SourceVersion: srcVersion,
		TargetVersion: tgtVersion,
	}

	// RDB-version directional gate.
	if srcVersion > 0 && tgtVersion > 0 && tgtVersion < srcVersion {
		r.Findings = append(r.Findings, engine.Finding{
			Severity: engine.SevBlock,
			Category: "rdb-version",
			Message: fmt.Sprintf("target Redis (%d) is older than source (%d): RESTORE rejects a "+
				"newer RDB payload on an older server (DUMP payload version or checksum are wrong). "+
				"Upgrade the target to at least the source version, or replicate the other direction "+
				"(redis-plan §0.2)", tgtVersion, srcVersion),
		})
	}

	// Module gate: every source module must be present on the target.
	if missing := missingModules(source, target); len(missing) > 0 {
		for _, m := range missing {
			r.Findings = append(r.Findings, engine.Finding{
				Severity: engine.SevBlock,
				Category: "missing-module",
				Message: fmt.Sprintf("module %q is loaded on the source but not the target: a value of a "+
					"module type cannot be RESTOREd without the identical module, and replicare never "+
					"reconstructs it (§1.7). Load %q on the target (redis-plan §0.2)", m, m),
			})
		}
	}

	// Every unit -> a replicable single-member acyclic component.
	if source != nil {
		for _, t := range source.Tables {
			if !t.HasUsableKey() {
				r.Skipped = append(r.Skipped, engine.SkippedTable{Table: t.Ref, Reason: "no usable key"})
				continue
			}
			r.Replicable = append(r.Replicable, t.Ref)
			r.Components = append(r.Components, engine.Component{
				Tables: []engine.TableRef{t.Ref},
				Order:  []engine.TableRef{t.Ref},
			})
		}
	}
	return r
}

// missingModules returns the module names loaded on the source but absent on the
// target.
func missingModules(source, target *engine.Schema) []string {
	if source == nil {
		return nil
	}
	have := map[string]bool{}
	if target != nil {
		for _, m := range target.Capabilities.Modules {
			have[m] = true
		}
	}
	var missing []string
	for _, m := range source.Capabilities.Modules {
		if !have[m] {
			missing = append(missing, m)
		}
	}
	return missing
}
