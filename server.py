from flask import Flask, Response, jsonify
from flask_cors import CORS
import psycopg2
import json
import datetime
import os

app = Flask(__name__)
CORS(app)  # Permitir CORS para o Kepler.gl acessar

DB_CONFIG = {
    "host": "xyz.render.com",
    "database": "redes_transporte",
    "user": "redes_transporte_user",
    "password": "s7aPZEPuTVUftvfU0ZukJHeqvUQUcZiV",
    "port": 5432
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def converter(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return o

def get_geojson_data():
    """Gerar GeoJSON com apenas empresa, prefixo e descrição, ignorando geometrias inválidas"""
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT jsonb_build_object(
                'type', 'FeatureCollection',
                'features', jsonb_agg(
                    jsonb_build_object(
                        'type', 'Feature',
                        'geometry', ST_AsGeoJSON(wkb_geometry)::jsonb,
                        'properties', jsonb_build_object(
                            'empresa', empresa,
                            'prefixo', prefixo,
                            'descrição', descrição
                        )
                    )
                )
            )
            FROM postgree
            WHERE wkb_geometry IS NOT NULL AND ST_IsValid(wkb_geometry);
        """)

        geojson_data = cur.fetchone()[0]

    except Exception as e:
        print("Erro ao gerar GeoJSON:")
        import traceback
        traceback.print_exc()
        geojson_data = {"type": "FeatureCollection", "features": []}  # retorna vazio em caso de erro

    finally:
        cur.close()
        conn.close()

    return geojson_data


@app.route("/dados.geojson")
def dados_geojson():
    """Endpoint que retorna GeoJSON para Kepler.gl e desativa cache"""
    try:
        geojson_data = get_geojson_data()
        return Response(
            json.dumps(geojson_data, default=converter, ensure_ascii=False),
            mimetype="application/geo+json",
            status=200,
            headers={
                'Access-Control-Allow-Origin': '*',        # CORS
                'Access-Control-Allow-Methods': 'GET',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',  # sem cache
                'Pragma': 'no-cache',                      # compatibilidade HTTP/1.0
                'Expires': '0'
            }
        )
    except Exception as e:
        import traceback
        print("Erro ao gerar GeoJSON:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/status")
def status():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM postgree WHERE wkb_geometry IS NOT NULL")
        total_routes = cur.fetchone()[0]

        cur.execute("SELECT DISTINCT empresa FROM postgree WHERE empresa IS NOT NULL")
        empresas = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            "status": "online",
            "total_routes": total_routes,
            "companies": empresas,
            "database": "PostgreSQL com PostGIS",
            "format": "GeoJSON"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.datetime.now().isoformat()})

if __name__ == "__main__":
    print("Iniciando aplicação Flask...")
    print("Aplicação pronta!")
    app.run(debug=True, host="0.0.0.0", port=5000)
