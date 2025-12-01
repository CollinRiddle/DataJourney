import os
import json
import csv
import logging
from datetime import datetime
from typing import Dict, Any, List

import sys
import psycopg2

def get_connection():
    uri = os.getenv("AIVEN_PG_URI") or os.getenv("DATABASE_URL")
    if not uri:
        raise ValueError("Missing AIVEN_PG_URI/DATABASE_URL environment variable")
    if uri.startswith("postgres://"):
        uri = uri.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(uri)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASEDIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PORTS_CONFIG_PATH = os.path.join(BASEDIR, 'data_config', 'ports_config.json')
AIS_SAMPLE_PATH = os.path.join(BASEDIR, 'data', 'ais_sample.csv')


def inside_bbox(lat: float, lon: float, bbox: Dict[str, float]) -> bool:
    return (
        bbox['min_lat'] <= lat <= bbox['max_lat'] and
        bbox['min_lon'] <= lon <= bbox['max_lon']
    )


def load_ports() -> List[Dict[str, Any]]:
    with open(PORTS_CONFIG_PATH, 'r') as f:
        data = json.load(f)
    return data['ports']


def load_positions() -> List[Dict[str, Any]]:
    positions: List[Dict[str, Any]] = []
    with open(AIS_SAMPLE_PATH, 'r', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                positions.append({
                    'vessel_id': row['vessel_id'],
                    'port_symbol': row['symbol'],
                    'lat': float(row['lat']),
                    'lon': float(row['lon']),
                    'speed_knots': float(row['speed_knots']),
                    'timestamp': datetime.fromisoformat(row['timestamp'].replace('Z', '+00:00')),
                })
            except Exception:
                continue
    return positions


def compute_metrics(ports: List[Dict[str, Any]], positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    now = datetime.utcnow()

    # Baselines (simple illustrative values)
    BASELINE = {
        'dwell_events': 3,
        'queue_size': 2,
        'throughput': 8,
    }

    for port in ports:
        port_id = port['port_id']
        port_name = port['name']
        port_bbox = port['port_bbox']
        anch_bbox = port['anchorage_bbox']

        # Filter positions relevant to this port
        port_positions = [p for p in positions if p['port_symbol'] == port_id]

        # Metrics
        dwell_events = 0
        queue_vessels = set()
        throughput_events = 0

        for p in port_positions:
            # Dwell/queue: stationary in anchorage
            if inside_bbox(p['lat'], p['lon'], anch_bbox) and p['speed_knots'] <= 0.5:
                dwell_events += 1
                queue_vessels.add(p['vessel_id'])

            # Throughput: moving inside port area
            if inside_bbox(p['lat'], p['lon'], port_bbox) and p['speed_knots'] >= 5.0:
                throughput_events += 1

        queue_size = len(queue_vessels)

        # Simple normalized components (0..1)
        dwell_ratio = min(1.0, dwell_events / max(1, BASELINE['dwell_events']))
        queue_ratio = min(1.0, queue_size / max(1, BASELINE['queue_size']))
        throughput_drop = 0.0
        if BASELINE['throughput'] > 0:
            throughput_drop = max(0.0, (BASELINE['throughput'] - throughput_events) / BASELINE['throughput'])

        # Risk score as weighted sum
        risk_score = (
            dwell_ratio * 40 +
            queue_ratio * 40 +
            throughput_drop * 20
        )

        drivers = {
            'dwell_events': dwell_events,
            'queue_size': queue_size,
            'throughput_events': throughput_events,
            'baseline': BASELINE,
            'components': {
                'dwell_ratio': round(dwell_ratio, 3),
                'queue_ratio': round(queue_ratio, 3),
                'throughput_drop': round(throughput_drop, 3)
            }
        }

        results.append({
            'port_id': port_id,
            'port_name': port_name,
            'score_time': now,
            'dwell_events': dwell_events,
            'queue_size': queue_size,
            'throughput': throughput_events,
            'risk_score': round(risk_score, 2),
            'drivers': json.dumps(drivers)
        })

    return results


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ports (
                port_id TEXT PRIMARY KEY,
                name TEXT,
                country TEXT,
                min_lat DOUBLE PRECISION,
                max_lat DOUBLE PRECISION,
                min_lon DOUBLE PRECISION,
                max_lon DOUBLE PRECISION,
                anch_min_lat DOUBLE PRECISION,
                anch_max_lat DOUBLE PRECISION,
                anch_min_lon DOUBLE PRECISION,
                anch_max_lon DOUBLE PRECISION
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS port_disruption_scores (
                port_id TEXT,
                port_name TEXT,
                score_time TIMESTAMP,
                dwell_events INTEGER,
                queue_size INTEGER,
                throughput INTEGER,
                risk_score DOUBLE PRECISION,
                drivers JSONB,
                PRIMARY KEY (port_id, score_time)
            )
            """
        )
        conn.commit()


def upsert_ports(conn, ports: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        for p in ports:
            cur.execute(
                """
                INSERT INTO ports (
                    port_id, name, country,
                    min_lat, max_lat, min_lon, max_lon,
                    anch_min_lat, anch_max_lat, anch_min_lon, anch_max_lon
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (port_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    country = EXCLUDED.country,
                    min_lat = EXCLUDED.min_lat,
                    max_lat = EXCLUDED.max_lat,
                    min_lon = EXCLUDED.min_lon,
                    max_lon = EXCLUDED.max_lon,
                    anch_min_lat = EXCLUDED.anch_min_lat,
                    anch_max_lat = EXCLUDED.anch_max_lat,
                    anch_min_lon = EXCLUDED.anch_min_lon,
                    anch_max_lon = EXCLUDED.anch_max_lon
                """,
                (
                    p['port_id'], p['name'], p['country'],
                    p['port_bbox']['min_lat'], p['port_bbox']['max_lat'], p['port_bbox']['min_lon'], p['port_bbox']['max_lon'],
                    p['anchorage_bbox']['min_lat'], p['anchorage_bbox']['max_lat'], p['anchorage_bbox']['min_lon'], p['anchorage_bbox']['max_lon']
                )
            )
        conn.commit()


def load_scores(conn, scores: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        for s in scores:
            cur.execute(
                """
                INSERT INTO port_disruption_scores (
                    port_id, port_name, score_time, dwell_events, queue_size, throughput, risk_score, drivers
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (port_id, score_time) DO NOTHING
                """,
                (
                    s['port_id'], s['port_name'], s['score_time'], s['dwell_events'], s['queue_size'], s['throughput'], s['risk_score'], s['drivers']
                )
            )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scores_time ON port_disruption_scores(score_time)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_scores_port ON port_disruption_scores(port_id)")
        conn.commit()


def main():
    logging.info('Initializing Global Shipping Disruption Monitoring pipeline')
    ports = load_ports()
    positions = load_positions()

    logging.info('Computing disruption metrics')
    scores = compute_metrics(ports, positions)

    conn = get_connection()
    ensure_tables(conn)
    upsert_ports(conn, ports)
    load_scores(conn, scores)
    conn.close()

    logging.info('✅ Pipeline completed. Inserted %d scores', len(scores))


if __name__ == '__main__':
    main()
