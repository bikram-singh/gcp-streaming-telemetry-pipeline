import argparse
import json
import logging
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions


class ParseAndValidateTelemetryFn(beam.DoFn):
    """Parses incoming JSON telemetry. Yields valid records or routes malformed JSON to DLQ."""

    TAG_DLQ = 'dlq'

    def process(self, element):
        try:
            payload = json.loads(element.decode('utf-8'))
            machine_id = payload.get('machine_id')
            temperature = payload.get('temperature')
            vibration = payload.get('vibration')
            timestamp_str = payload.get('timestamp')

            # Ensure required fields exist and types are correct
            if not machine_id or temperature is None or vibration is None or not timestamp_str:
                yield beam.pvalue.TaggedOutput(self.TAG_DLQ, element)
                return

            parsed_data = {
                'machine_id': str(machine_id),
                'temperature': float(temperature),
                'vibration': float(vibration),
                'timestamp': timestamp_str
            }
            yield parsed_data
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            logging.warning(f"Malformed payload encountered: {element}. Error: {e}")
            yield beam.pvalue.TaggedOutput(self.TAG_DLQ, element)


class TelemetryCombineFn(beam.CombineFn):
    """Accumulates sum and count for both temperature and vibration metrics."""

    def create_accumulator(self):
        # (temp_sum, temp_count, vib_sum, vib_count)
        return (0.0, 0, 0.0, 0)

    def add_input(self, accumulator, element):
        temp_sum, temp_count, vib_sum, vib_count = accumulator
        return (
            temp_sum + element['temperature'],
            temp_count + 1,
            vib_sum + element['vibration'],
            vib_count + 1
        )

    def merge_accumulators(self, accumulators):
        temp_sums, temp_counts, vib_sums, vib_counts = zip(*accumulators)
        return (sum(temp_sums), sum(temp_counts), sum(vib_sums), sum(vib_counts))

    def extract_output(self, accumulator):
        temp_sum, temp_count, vib_sum, vib_count = accumulator
        avg_temp = temp_sum / temp_count if temp_count > 0 else 0.0
        avg_vib = vib_sum / vib_count if vib_count > 0 else 0.0
        return {
            'avg_temperature': avg_temp,
            'avg_vibration': avg_vib
        }


class FormatBQFn(beam.DoFn):
    """Formats aggregated metrics into BigQuery schema structure using window parameters.

    Both timestamp fields are explicitly marked UTC ('Z' suffix) for unambiguous
    parsing on the BigQuery side and downstream (this value is reused verbatim
    as `detected_at` in the Gemini diagnostics Cloud Function).
    """

    def process(self, element, window=beam.DoFn.WindowParam):
        machine_id, agg_metrics = element
        window_end_iso = window.end.to_utc_datetime().isoformat() + 'Z'
        calculated_at_iso = datetime.now(timezone.utc).isoformat()

        yield {
            'machine_id': machine_id,
            'avg_temperature': round(agg_metrics['avg_temperature'], 2),
            'avg_vibration': round(agg_metrics['avg_vibration'], 2),
            'window_end': window_end_iso,
            'calculated_at': calculated_at_iso
        }


class FilterAndFormatAlertsFn(beam.DoFn):
    """Filters records exceeding safe thresholds and formats alerts for Pub/Sub topic."""

    TAG_ALERT = 'alert'

    def process(self, element):
        avg_temp = element['avg_temperature']
        avg_vib = element['avg_vibration']

        # Threshold Rule: avg_temperature > 80.0 OR avg_vibration > 5.0
        if avg_temp > 80.0 or avg_vib > 5.0:
            alert_payload = {
                'machine_id': element['machine_id'],
                'avg_temperature': avg_temp,
                'avg_vibration': avg_vib,
                'window_end': element['window_end']
            }
            # Encode JSON bytes for Pub/Sub write
            encoded_payload = json.dumps(alert_payload).encode('utf-8')
            yield beam.pvalue.TaggedOutput(self.TAG_ALERT, encoded_payload)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_subscription', required=True, help='Pub/Sub subscription to pull from.')
    parser.add_argument('--dlq_topic', required=True, help='Pub/Sub topic for malformed payloads.')
    parser.add_argument('--alerts_topic', required=True, help='Pub/Sub topic for anomaly alerts.')
    parser.add_argument('--bq_table', required=True, help='BigQuery table for aggregated telemetry.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read from Pub/Sub Input Subscription
        raw_telemetry = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)

        # 2. Parse & Route Malformed Records to DLQ
        parsed_results = raw_telemetry | "ParseAndValidate" >> beam.ParDo(ParseAndValidateTelemetryFn()).with_outputs(
            ParseAndValidateTelemetryFn.TAG_DLQ, main='valid'
        )

        # Write DLQ Records directly
        _ = parsed_results[ParseAndValidateTelemetryFn.TAG_DLQ] | "WriteToDLQ" >> beam.io.WriteToPubSub(topic=known_args.dlq_topic)

        # 3. Apply 1-Minute Fixed Windows (Tumbling Windows)
        windowed_telemetry = parsed_results['valid'] | "Apply1MinFixedWindows" >> beam.WindowInto(
            beam.window.FixedWindows(60)
        )

        # 4. Key by machine_id and Aggregate Metrics
        aggregates = (
            windowed_telemetry
            | "KeyByMachineId" >> beam.Map(lambda x: (x['machine_id'], x))
            | "CombinePerMachine" >> beam.CombinePerKey(TelemetryCombineFn())
        )

        # 5. Format Aggregates for BigQuery
        formatted_bq_rows = aggregates | "FormatForBQ" >> beam.ParDo(FormatBQFn())

        # 6. Write Aggregates to BigQuery
        _ = formatted_bq_rows | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=known_args.bq_table,
            schema='machine_id:STRING,avg_temperature:FLOAT,avg_vibration:FLOAT,window_end:TIMESTAMP,calculated_at:TIMESTAMP',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # 7. Evaluate Threshold Breaches and Write Alerts
        alerts = formatted_bq_rows | "FilterThresholdBreaches" >> beam.ParDo(FilterAndFormatAlertsFn()).with_outputs(
            FilterAndFormatAlertsFn.TAG_ALERT
        )

        _ = alerts[FilterAndFormatAlertsFn.TAG_ALERT] | "PublishAlerts" >> beam.io.WriteToPubSub(topic=known_args.alerts_topic)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
