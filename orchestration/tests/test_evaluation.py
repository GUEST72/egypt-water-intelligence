from water_orchestration.evaluation import evaluate

TRUTH = [
    {"scenario_id": "burst-d1", "district_id": "D001", "scenario_type": "burst",
     "start_time_utc": "2026-01-02T06:00:00Z", "end_time_utc": "2026-01-02T08:00:00Z"},
    {"scenario_id": "background-loss-d6", "district_id": "D006", "scenario_type": "persistent_background_loss",
     "start_time_utc": "2026-01-01T00:00:00Z", "end_time_utc": "2026-01-26T00:00:00Z"},
]


def alert(alert_id, district, alert_type="flow_anomaly", status="open", first="2026-01-02T06:00:00",
          window_start="2026-01-02T06:15:00", window_end="2026-01-02T06:30:00"):
    return {"alert_id": alert_id, "district_id": district, "alert_type": alert_type, "status": status,
            "first_flagged_window_start": first, "window_start": window_start, "window_end": window_end}


def test_detection_delay_false_alarm_and_undetectable_loss():
    alerts = [alert("a1", "D001"), alert("a1", "D001"),  # replayed duplicate
              alert("a2", "D003", first="2026-01-05T00:00:00")]
    result = evaluate(alerts, TRUTH, [], "2026-01-01T00:00:00")
    burst, loss = result["scenarios"]
    assert burst["detected"] and burst["type_ok"] and burst["delay_min"] == 30
    assert not loss["detectable"] and not loss["detected"]
    s = result["summary"]
    assert (s["alerts"], s["false_alarms"], s["detectable_detected"], s["scenarios_detectable"]) == (2, 1, 1, 1)


def test_data_quality_fault_matched_to_its_window():
    faults = [{"stream": "district", "fault_type": "null_value", "event_id": "run-x:district:D002:000017"}]
    dq = alert("q1", "D002", alert_type="data_quality", status="instant", first=None,
               window_start="2026-01-01T00:15:00")
    s = evaluate([dq], [], faults, "2026-01-01T00:00:00")["summary"]
    assert (s["dq_faults"], s["dq_alerted"], s["dq_missed"]) == (1, 1, 0)
