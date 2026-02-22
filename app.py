#!/usr/bin/env python3
"""
FGS App — Serveur avec base de données PostgreSQL
==================================================

Variables d'environnement requises sur Render :
  DATABASE_URL  -> fournie automatiquement par Render PostgreSQL
  PORT          -> fournie automatiquement par Render
  [identifiant] -> une variable par utilisateur, valeur = mot de passe
"""

import json, os
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__, static_folder=".")

# ── Connexion ─────────────────────────────────────────────────────────────────

def get_conn():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL manquante.")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)

# ── Schéma et données par défaut ──────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS fgs_data (
    id     SERIAL PRIMARY KEY,
    cle    TEXT UNIQUE NOT NULL,
    valeur JSONB NOT NULL,
    maj    TIMESTAMPTZ DEFAULT NOW()
);
"""

DONNEES_DEFAUT = {
    "categories": [
        {"id":"c1","nom":"Pelles","icone":"🦾"},
        {"id":"c2","nom":"Projection voie sèche","icone":"💨"},
        {"id":"c3","nom":"Injection","icone":"💉"},
        {"id":"c4","nom":"Manutention","icone":"🏗️"},
        {"id":"c5","nom":"Foreuses","icone":"🔩"},
        {"id":"c6","nom":"Glissières","icone":"🛤️"},
        {"id":"c7","nom":"Disqueuses thermiques","icone":"⚙️"},
    ],
    "machines": [
        {"id":"EQ-001","catId":"c1","nom":"Minipelle 2,5t","modele":"Kubota U25-3",
         "annee":2021,"heures":1620,"hEntretien":1500,"seuil":250,"vgp":"2026-12-03",
         "site":"Lyon","serie":"KB-U25-3-XYZ","poids":2500,
         "piecesAssociees":[
             {"pieceId":"P-001","heuresInstallation":1500,"dateInstallation":"2026-01-10"},
             {"pieceId":"P-003","heuresInstallation":1200,"dateInstallation":"2025-10-01"},
         ]},
        {"id":"EQ-002","catId":"c4","nom":"Chariot télescopique","modele":"Manitou MT625",
         "annee":2020,"heures":3100,"hEntretien":2900,"seuil":200,"vgp":"2026-04-15",
         "site":"Grenoble","piecesAssociees":[]},
    ],
    "pieces": [
        {"id":"P-001","nom":"Filtre huile moteur","ref":"FH-123","dureeVal":250,"dureeUnite":"heures","stock":3,"notes":"Utiliser pièce d'origine"},
        {"id":"P-002","nom":"Courroie de distribution","ref":"CR-321","dureeVal":500,"dureeUnite":"heures","stock":1},
        {"id":"P-003","nom":"Filtre à air","ref":"FA-456","dureeVal":500,"dureeUnite":"heures","stock":2},
        {"id":"P-004","nom":"Filtre hydraulique","ref":"FH-789","dureeVal":1000,"dureeUnite":"heures","stock":0},
    ],
    "interventions": [
        {"id":"I-001","machineId":"EQ-001","date":"2026-01-10","datePrevue":"","heures":1500,
         "type":"entretien","notes":"Vidange + filtre huile","piecesChangees":["P-001"]},
        {"id":"I-002","machineId":"EQ-002","date":"2026-02-01","datePrevue":"","heures":2900,
         "type":"entretien","notes":"Révision à 2900h","piecesChangees":[]},
    ],
    "livraisons": [],
}

def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)
            cur.execute("SELECT COUNT(*) FROM fgs_data WHERE cle = 'bd'")
            if cur.fetchone()[0] == 0:
                cur.execute(
                    "INSERT INTO fgs_data (cle, valeur) VALUES (%s, %s)",
                    ("bd", json.dumps(DONNEES_DEFAUT, ensure_ascii=False))
                )
        conn.commit()
    print("Base PostgreSQL initialisee.")

# ── Lecture / écriture ────────────────────────────────────────────────────────

def lire():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT valeur FROM fgs_data WHERE cle = 'bd'")
            row = cur.fetchone()
            if row is None:
                init_db()
                return DONNEES_DEFAUT
            val = row["valeur"]
            return val if isinstance(val, dict) else json.loads(val)

def ecrire(bd):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fgs_data (cle, valeur, maj) VALUES (%s, %s, NOW())
                ON CONFLICT (cle) DO UPDATE SET valeur = EXCLUDED.valeur, maj = NOW()
                """,
                ("bd", json.dumps(bd, ensure_ascii=False))
            )
        conn.commit()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "app.html")

@app.route("/api/auth", methods=["POST"])
def auth():
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"ok": False}), 400
        identifiant = data.get("identifiant", "").strip()
        mdp = data.get("mdp", "")
        if not identifiant or not mdp:
            return jsonify({"ok": False}), 400
        for key, value in os.environ.items():
            if key.lower() == identifiant.lower() and value == mdp:
                return jsonify({"ok": True, "nom": key})
        return jsonify({"ok": False})
    except Exception as e:
        return jsonify({"ok": False, "erreur": str(e)}), 500

@app.route("/api/bd", methods=["GET"])
def get_bd():
    try:
        return jsonify(lire())
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500

@app.route("/api/bd", methods=["POST"])
def post_bd():
    try:
        bd = request.get_json(force=True, silent=True)
        if bd is None:
            return jsonify({"erreur": "JSON invalide"}), 400
        ecrire(bd)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500

@app.route("/api/sante")
def sante():
    """Diagnostic : verifie que la base repond."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT maj FROM fgs_data WHERE cle='bd'")
                row = cur.fetchone()
                derniere_maj = str(row[0]) if row else "jamais"
        return jsonify({"ok": True, "derniere_maj": derniere_maj})
    except Exception as e:
        return jsonify({"ok": False, "erreur": str(e)}), 500

# ── Démarrage ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    print(f"Serveur demarre sur http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
