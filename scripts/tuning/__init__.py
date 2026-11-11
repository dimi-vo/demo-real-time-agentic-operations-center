"""Offline tuning harness for the anomaly detectors (developer tool, D-031).

`arima` replicates ML_DETECT_ANOMALIES closely enough to compare configurations;
`calibrate` checks the replica against what Confluent Cloud actually computed (the
expected values and bounds stored in service_health); `sweep` runs the real generator
code at any settings through the replica and scores it with evaluate's own scoring.
"""
