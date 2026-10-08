// Remote shadow projection for the monitor (Phase 3).
//
// LoadRemoteShadows reads a controller's remote_shadows table (same column
// names as src/vanth/remote/store.py CONTROLLER_DDL) and projects them into
// JobSummary rows. Only current-timeline shadows are returned: rows that are
// suppressed (forgotten) or superseded (old snapshot epoch) are filtered out,
// and each row is pinned to the remote's current state_epoch.
package monitor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// ShadowRow is one projected remote job shadow.
type ShadowRow struct {
	RemoteID    string
	JobID       string
	Status      string
	Name        string
	Command     string
	UpdatedAt   string
	CreatedAt   string
	StateEpoch  int64
}

// LoadRemoteShadows opens dbPath read-only and returns the current-timeline
// shadows for every remote, merged across remotes and ordered by creation.
func LoadRemoteShadows(dbPath string) ([]ShadowRow, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro&_pragma=query_only(1)", dbPath))
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
		SELECT s.remote_id, s.remote_job_id, s.status, COALESCE(s.payload_json,''),
		       s.created_at, s.updated_at, s.state_epoch
		FROM remote_shadows s
		JOIN remotes r ON r.remote_id = s.remote_id
		WHERE s.suppressed_at IS NULL
		  AND s.superseded_at IS NULL
		  AND s.state_epoch = r.state_epoch
		ORDER BY s.created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ShadowRow
	for rows.Next() {
		var row ShadowRow
		var payload string
		if err := rows.Scan(&row.RemoteID, &row.JobID, &row.Status, &payload,
			&row.CreatedAt, &row.UpdatedAt, &row.StateEpoch); err != nil {
			return nil, err
		}
		row.Name = extractPayloadName(payload)
		row.Command = extractPayloadCommand(payload)
		out = append(out, row)
	}
	return out, rows.Err()
}

// extractPayloadName pulls "name" from a shadow payload_json, falling back to
// empty on any parse problem.
func extractPayloadName(payload string) string {
	return extractPayloadString(payload, "name")
}

func extractPayloadCommand(payload string) string {
	return extractPayloadString(payload, "command")
}

// extractPayloadString decodes one top-level string field with encoding/json.
// (A previous hand-rolled substring scan mishandled every escape except \"
// — \\, \n, \uXXXX — and silently returned truncated values.)
func extractPayloadString(payload, key string) string {
	if payload == "" {
		return ""
	}
	var obj map[string]any
	dec := json.NewDecoder(strings.NewReader(payload))
	if err := dec.Decode(&obj); err != nil {
		return ""
	}
	value, _ := obj[key].(string)
	return value
}

// ProjectShadows converts shadow rows into JobSummary entries for the view.
func ProjectShadows(rows []ShadowRow) []JobSummary {
	out := make([]JobSummary, 0, len(rows))
	for _, row := range rows {
		status := row.Status
		if status == "" {
			status = "submitting"
		}
		out = append(out, JobSummary{
			JobID:     row.JobID,
			Name:      row.Name,
			Command:   row.Command,
			Status:    status,
			CreatedAt: row.CreatedAt,
			UpdatedAt: row.UpdatedAt,
			RemoteID:  row.RemoteID,
			Shadow:    true,
		})
	}
	return out
}
