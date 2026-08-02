package mysql

import (
	"strings"
	"testing"
)

func TestDeltaTableDDLShape(t *testing.T) {
	ddl := deltaTableDDL(7, []captureCol{{Name: "id", Type: "int"}, {Name: "sub", Type: "varchar(20)"}})
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `replicare`.`delta_7`",
		"delta_id BIGINT AUTO_INCREMENT PRIMARY KEY",
		"`k1` int NULL", "`k2` varchar(20) NULL",
		"ENGINE=InnoDB",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("delta DDL missing %q:\n%s", want, ddl)
		}
	}
}

func TestUpdateTriggerEnqueuesBothOnKeyChange(t *testing.T) {
	ddl := triggerDDL(7, "app", "orders", 'U', []captureCol{{Name: "id", Type: "int"}})
	// NULL-safe change detection.
	if !strings.Contains(ddl, "NOT (NEW.`id` <=> OLD.`id`)") {
		t.Errorf("update trigger missing null-safe key-change check:\n%s", ddl)
	}
	// On change: OLD as 'D' then NEW as 'U'.
	if !strings.Contains(ddl, "VALUES ('D', OLD.`id`)") || !strings.Contains(ddl, "VALUES ('U', NEW.`id`)") {
		t.Errorf("update trigger should enqueue OLD as D and NEW as U:\n%s", ddl)
	}
	if !strings.Contains(ddl, "DEFINER = CURRENT_USER") {
		t.Errorf("trigger should be DEFINER = CURRENT_USER:\n%s", ddl)
	}
	if !strings.Contains(ddl, "AFTER UPDATE ON `app`.`orders`") {
		t.Errorf("trigger target wrong:\n%s", ddl)
	}
}

func TestInsertDeleteTriggerShape(t *testing.T) {
	ins := triggerDDL(3, "app", "t", 'I', []captureCol{{Name: "id", Type: "int"}})
	if !strings.Contains(ins, "AFTER INSERT") || !strings.Contains(ins, "VALUES ('I', NEW.`id`)") {
		t.Errorf("insert trigger shape:\n%s", ins)
	}
	del := triggerDDL(3, "app", "t", 'D', []captureCol{{Name: "id", Type: "int"}})
	if !strings.Contains(del, "AFTER DELETE") || !strings.Contains(del, "VALUES ('D', OLD.`id`)") {
		t.Errorf("delete trigger shape:\n%s", del)
	}
}

func TestTriggerNames(t *testing.T) {
	if got := triggerName(5, 'I'); got != "rc_trg_i_5" {
		t.Errorf("triggerName I = %q", got)
	}
	if got := triggerName(5, 'U'); got != "rc_trg_u_5" {
		t.Errorf("triggerName U = %q", got)
	}
}
