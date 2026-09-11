from google.cloud import bigquery
c = bigquery.Client(project='project-streaming-telemetry')
query = "SELECT * FROM `telemetry_analytics.incident_log` ORDER BY detected_at DESC LIMIT 5"
for row in c.query(query).result():
    print(dict(row))
