from google.cloud import bigquery
c = bigquery.Client(project='project-streaming-telemetry')
query = "SELECT * FROM `telemetry_analytics.telemetry_aggregates` ORDER BY calculated_at DESC LIMIT 10"
for row in c.query(query).result():
    print(dict(row))
