#!/usr/bin/env python3
"""
FGS App — Serveur avec base de données PostgreSQL
Utilise pg8000 (compatible Python 3.14+)
"""

import json, os, re
import pg8000.native
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__, static_folder=".")

# ── Connexion ─────────────────────────────────────────────────────────────────

def parse_db_url(url):
    """Parse une DATABASE_URL en paramètres de connexion."""
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    m = re.match(r'postgresql://([^:]+):([^@]+)@([^:/]+):?(\d*)/(.+)', url)
    if not m:
        raise ValueError(f"DATABASE_URL invalide : {url}")
    user, password, host, port, dbname = m.groups()
    return {
        "user": user,
        "password": password,
        "host": host,
        "port": int(port) if port else 5432,
        "database": dbname.split("?")[0],  # ignore ?sslmode=...
        "ssl_context": True,               # Render exige SSL
    }

def get_conn():
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL manquante.")
    p = parse_db_url(url)
    return pg8000.native.Connection(**p)

# ── Schéma et données par défaut ──────────────────────────────────────────────

SCHEMA_BD = """
CREATE TABLE IF NOT EXISTS fgs_data (
    id     SERIAL PRIMARY KEY,
    cle    TEXT UNIQUE NOT NULL,
    valeur TEXT NOT NULL,
    maj    TIMESTAMPTZ DEFAULT NOW()
)
"""

SCHEMA_POINTAGE = """
CREATE TABLE IF NOT EXISTS pointages (
    id          SERIAL PRIMARY KEY,
    date_jour   DATE NOT NULL,
    chantier    TEXT NOT NULL,
    auteur      TEXT NOT NULL,
    role_auteur TEXT NOT NULL DEFAULT 'chef',
    lignes      JSONB NOT NULL DEFAULT '[]',
    cree_le     TIMESTAMPTZ DEFAULT NOW(),
    maj_le      TIMESTAMPTZ DEFAULT NOW()
)
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
    conn = get_conn()
    conn.run(SCHEMA_BD)
    conn.run(SCHEMA_POINTAGE)
    rows = conn.run("SELECT COUNT(*) FROM fgs_data WHERE cle = 'bd'")
    if rows[0][0] == 0:
        conn.run(
            "INSERT INTO fgs_data (cle, valeur) VALUES (:cle, :valeur)",
            cle="bd",
            valeur=json.dumps(DONNEES_DEFAUT, ensure_ascii=False)
        )
    conn.close()
    print("Base PostgreSQL initialisee (fgs_data + pointages).")

# ── Lecture / écriture ────────────────────────────────────────────────────────

def lire():
    conn = get_conn()
    rows = conn.run("SELECT valeur FROM fgs_data WHERE cle = 'bd'")
    conn.close()
    if not rows:
        init_db()
        return DONNEES_DEFAUT
    val = rows[0][0]
    return json.loads(val) if isinstance(val, str) else val

def ecrire(bd):
    conn = get_conn()
    conn.run(
        """
        INSERT INTO fgs_data (cle, valeur, maj) VALUES (:cle, :valeur, NOW())
        ON CONFLICT (cle) DO UPDATE SET valeur = EXCLUDED.valeur, maj = NOW()
        """,
        cle="bd",
        valeur=json.dumps(bd, ensure_ascii=False)
    )
    conn.close()

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "app.html")


def get_role(identifiant):
    """
    Retourne le rôle d'un utilisateur depuis ses variables d'environnement.
    Format de la variable : identifiant=motdepasse:role
    Rôles : admin, rh, chef
    Exemple : Admin=MonMDP:admin  /  rh.martin=MDP:rh  /  chef.lyon=MDP:chef
    Compatibilité ancien format (juste mot de passe sans rôle) = chef par défaut
    """
    for key, value in os.environ.items():
        if key.lower() == identifiant.lower():
            parts = value.split(":")
            if len(parts) >= 2:
                return parts[-1].strip().lower()  # dernier segment = rôle
            return "chef"  # ancien format sans rôle = chef par défaut
    return "chef"


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
            if key.lower() != identifiant.lower():
                continue
            value = value.strip()
            # Format nouveau : motdepasse:role (ex: MonMDP:chef ou MonMDP:admin)
            # Format ancien  : motdepasse seul (ex: Illite@8020)
            # Cas spécial    : le mdp lui-même contient des ":" (ex: Illite@8020)
            # => on vérifie d'abord si la valeur entière correspond au mdp (ancien format)
            # => sinon on sépare sur le dernier ":" pour extraire le rôle
            role = "chef"  # défaut
            if value == mdp:
                # Ancien format exact — pas de rôle défini
                mdp_stocke = value
            elif ":" in value:
                # Nouveau format : le rôle est le dernier segment après ":"
                last_colon = value.rfind(":")
                mdp_stocke = value[:last_colon]
                role_candidat = value[last_colon+1:].strip().lower()
                if role_candidat in ("chef", "admin", "rh"):
                    role = role_candidat
                else:
                    # Le ":" fait partie du mot de passe (ex: MonM:DP sans role)
                    mdp_stocke = value
            else:
                mdp_stocke = value
            if mdp_stocke == mdp:
                return jsonify({"ok": True, "nom": key, "role": role})
        return jsonify({"ok": False})
    except Exception as e:
        return jsonify({"ok": False, "erreur": str(e)}), 500


@app.route("/api/pointages", methods=["GET"])
def get_pointages():
    """Retourne les pointages selon le rôle."""
    try:
        role   = request.args.get("role", "chef")
        auteur = request.args.get("auteur", "")
        date_debut = request.args.get("debut", "")
        date_fin   = request.args.get("fin", "")

        conn = get_conn()
        if role in ("admin", "rh"):
            if date_debut and date_fin:
                rows = conn.run(
                    "SELECT * FROM pointages WHERE date_jour BETWEEN :d AND :f ORDER BY date_jour DESC, cree_le DESC",
                    d=date_debut, f=date_fin
                )
            else:
                rows = conn.run("SELECT * FROM pointages ORDER BY date_jour DESC, cree_le DESC LIMIT 200")
        else:
            # Chef : uniquement ses propres pointages
            if date_debut and date_fin:
                rows = conn.run(
                    "SELECT * FROM pointages WHERE auteur ILIKE :a AND date_jour BETWEEN :d AND :f ORDER BY date_jour DESC",
                    a=auteur, d=date_debut, f=date_fin
                )
            else:
                rows = conn.run(
                    "SELECT * FROM pointages WHERE auteur ILIKE :a ORDER BY date_jour DESC LIMIT 100",
                    a=auteur
                )
        conn.close()

        cols = ["id","date_jour","chantier","auteur","role_auteur","lignes","cree_le","maj_le"]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date_jour"] = str(d["date_jour"])
            d["cree_le"]   = str(d["cree_le"])
            d["maj_le"]    = str(d["maj_le"])
            if isinstance(d["lignes"], str):
                d["lignes"] = json.loads(d["lignes"])
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/pointages", methods=["POST"])
def save_pointage():
    """Crée ou met à jour un pointage."""
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"erreur": "Données invalides"}), 400

        pid        = data.get("id")
        date_jour  = data.get("date_jour")
        chantier   = data.get("chantier", "")
        auteur     = data.get("auteur", "")
        role_auteur= data.get("role_auteur", "chef")
        lignes     = data.get("lignes", [])

        conn = get_conn()
        if pid:
            conn.run(
                "UPDATE pointages SET lignes=:l, chantier=:c, maj_le=NOW() WHERE id=:id",
                l=json.dumps(lignes, ensure_ascii=False), c=chantier, id=pid
            )
            new_id = pid
        else:
            rows = conn.run(
                """INSERT INTO pointages (date_jour, chantier, auteur, role_auteur, lignes)
                   VALUES (:d, :c, :a, :r, :l) RETURNING id""",
                d=date_jour, c=chantier, a=auteur, r=role_auteur,
                l=json.dumps(lignes, ensure_ascii=False)
            )
            new_id = rows[0][0]
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/pointages/<int:pid>", methods=["DELETE"])
def delete_pointage(pid):
    try:
        role = request.args.get("role", "chef")
        auteur = request.args.get("auteur", "")
        conn = get_conn()
        if role in ("admin", "rh"):
            conn.run("DELETE FROM pointages WHERE id=:id", id=pid)
        else:
            conn.run("DELETE FROM pointages WHERE id=:id AND auteur ILIKE :a", id=pid, a=auteur)
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500




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


@app.route("/api/backup")
def backup():
    """
    Télécharge une sauvegarde complète des données au format JSON.
    Protégé par une clé secrète : /api/backup?key=VOTRE_CLE
    
    Sur Render : ajoutez une variable d'environnement BACKUP_KEY = votre_mot_de_passe_secret
    """
    cle = request.args.get("key", "")
    cle_attendue = os.environ.get("BACKUP_KEY", "")
    
    if not cle_attendue:
        return jsonify({"erreur": "Variable BACKUP_KEY non configurée sur Render."}), 500
    if cle != cle_attendue:
        return jsonify({"erreur": "Clé incorrecte."}), 403
    
    try:
        bd = lire()
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M")
        nom_fichier = f"fgs-backup-{date_str}.json"
        contenu = json.dumps(bd, ensure_ascii=False, indent=2)
        
        from flask import Response
        return Response(
            contenu,
            mimetype="application/json",
            headers={"Content-Disposition": f"attachment; filename={nom_fichier}"}
        )
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/restore", methods=["POST"])
def restore():
    """
    Restaure les données depuis un fichier JSON.
    Protégé par la même clé secrète BACKUP_KEY.
    
    Utilisation : POST /api/restore?key=VOTRE_CLE
    Corps : le fichier JSON de sauvegarde
    """
    cle = request.args.get("key", "")
    cle_attendue = os.environ.get("BACKUP_KEY", "")
    
    if not cle_attendue:
        return jsonify({"erreur": "Variable BACKUP_KEY non configurée sur Render."}), 500
    if cle != cle_attendue:
        return jsonify({"erreur": "Clé incorrecte."}), 403
    
    try:
        bd = request.get_json(force=True, silent=True)
        if not bd:
            return jsonify({"erreur": "Fichier JSON invalide ou vide."}), 400
        
        # Vérification basique que c'est bien une sauvegarde FGS
        champs_requis = ["categories", "machines", "pieces", "interventions", "livraisons"]
        for champ in champs_requis:
            if champ not in bd:
                return jsonify({"erreur": f"Sauvegarde invalide : champ '{champ}' manquant."}), 400
        
        ecrire(bd)
        return jsonify({
            "ok": True,
            "message": f"Restauration réussie : {len(bd.get('machines',[]))} machines, {len(bd.get('pieces',[]))} pièces, {len(bd.get('interventions',[]))} interventions."
        })
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500

@app.route("/api/sante")
def sante():
    try:
        conn = get_conn()
        rows = conn.run("SELECT maj FROM fgs_data WHERE cle='bd'")
        conn.close()
        derniere_maj = str(rows[0][0]) if rows else "jamais"
        return jsonify({"ok": True, "derniere_maj": derniere_maj})
    except Exception as e:
        return jsonify({"ok": False, "erreur": str(e)}), 500

# ── Démarrage ─────────────────────────────────────────────────────────────────

# ── Initialisation au démarrage (fonctionne avec Gunicorn ET python direct) ──
# Appelé à l'import du module — crée la table si elle n'existe pas
try:
    init_db()
except Exception as e:
    print(f"AVERTISSEMENT init_db: {e}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"Serveur demarre sur http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
