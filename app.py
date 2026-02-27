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

SCHEMA_LIVCHANTIER = """
CREATE TABLE IF NOT EXISTS livraisons_chantier (
    id           SERIAL PRIMARY KEY,
    date_liv     DATE NOT NULL,
    chantier     TEXT NOT NULL,
    auteur       TEXT NOT NULL,
    element      TEXT NOT NULL,
    quantite     NUMERIC(12,3) NOT NULL DEFAULT 0,
    unite        TEXT NOT NULL DEFAULT 'm3',
    notes        TEXT DEFAULT '',
    cree_le      TIMESTAMPTZ DEFAULT NOW()
)
"""

SCHEMA_PRIX_REF = """
CREATE TABLE IF NOT EXISTS prix_reference (
    id        SERIAL PRIMARY KEY,
    chantier  TEXT NOT NULL,
    element   TEXT NOT NULL,
    prix_unitaire NUMERIC(12,2) NOT NULL,
    maj_le    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(chantier, element)
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
    conn.run(SCHEMA_LIVCHANTIER)
    conn.run(SCHEMA_PRIX_REF)
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


@app.route("/api/livchantier", methods=["GET"])
def get_livchantier():
    try:
        role    = request.args.get("role", "chef")
        auteur  = request.args.get("auteur", "")
        chantier= request.args.get("chantier", "")
        element = request.args.get("element", "")
        chef_f  = request.args.get("chef", "")
        debut   = request.args.get("debut", "")
        fin     = request.args.get("fin", "")

        where = []
        params = {}
        if role not in ("admin", "rh"):
            where.append("auteur ILIKE :auteur")
            params["auteur"] = auteur
        if chantier:
            where.append("chantier ILIKE :chantier")
            params["chantier"] = f"%{chantier}%"
        if element:
            where.append("element = :element")
            params["element"] = element
        if chef_f:
            where.append("auteur ILIKE :chef_f")
            params["chef_f"] = f"%{chef_f}%"
        if debut and fin:
            where.append("date_liv BETWEEN :debut AND :fin")
            params["debut"] = debut
            params["fin"] = fin

        sql = """SELECT l.id, l.date_liv, l.chantier, l.auteur, l.element,
                        l.quantite, l.unite, p.prix_unitaire, l.notes, l.cree_le
                 FROM livraisons_chantier l
                 LEFT JOIN prix_reference p ON p.chantier=l.chantier AND p.element=l.element"""
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY date_liv DESC, cree_le DESC"

        conn = get_conn()
        rows = conn.run(sql, **params) if params else conn.run(sql)
        conn.close()

        cols = ["id","date_liv","chantier","auteur","element","quantite","unite","prix_unitaire","notes","cree_le"]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date_liv"] = str(d["date_liv"])
            d["cree_le"]  = str(d["cree_le"])
            d["quantite"] = float(d["quantite"]) if d["quantite"] is not None else 0
            d["prix_unitaire"] = float(d["prix_unitaire"]) if d["prix_unitaire"] is not None else None
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/livchantier", methods=["POST"])
def save_livchantier():
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"erreur": "Données invalides"}), 400
        lid      = data.get("id")
        date_liv = data.get("date_liv")
        chantier = data.get("chantier", "")
        auteur   = data.get("auteur", "")
        element  = data.get("element", "béton")
        quantite = float(data.get("quantite", 0))
        unite    = data.get("unite", "m3")
        notes    = data.get("notes", "")
        conn = get_conn()
        if lid:
            conn.run(
                """UPDATE livraisons_chantier
                   SET date_liv=:d, chantier=:c, element=:e, quantite=:q, unite=:u, notes=:n
                   WHERE id=:id""",
                d=date_liv, c=chantier, e=element, q=quantite, u=unite, n=notes, id=lid
            )
            new_id = lid
        else:
            rows = conn.run(
                """INSERT INTO livraisons_chantier
                   (date_liv,chantier,auteur,element,quantite,unite,notes)
                   VALUES (:d,:c,:a,:e,:q,:u,:n) RETURNING id""",
                d=date_liv, c=chantier, a=auteur, e=element, q=quantite, u=unite, n=notes
            )
            new_id = rows[0][0]
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/livchantier/<int:lid>", methods=["DELETE"])
def delete_livchantier(lid):
    try:
        role   = request.args.get("role", "chef")
        auteur = request.args.get("auteur", "")
        conn = get_conn()
        if role in ("admin", "rh"):
            conn.run("DELETE FROM livraisons_chantier WHERE id=:id", id=lid)
        else:
            conn.run("DELETE FROM livraisons_chantier WHERE id=:id AND auteur ILIKE :a",
                     id=lid, a=auteur)
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/prix_reference", methods=["GET"])
def get_prix_ref():
    """Retourne tous les prix de référence (chantier x element)."""
    try:
        conn = get_conn()
        rows = conn.run("SELECT id, chantier, element, prix_unitaire, maj_le FROM prix_reference ORDER BY chantier, element")
        conn.close()
        cols = ["id","chantier","element","prix_unitaire","maj_le"]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["prix_unitaire"] = float(d["prix_unitaire"]) if d["prix_unitaire"] else None
            d["maj_le"] = str(d["maj_le"])
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/prix_reference", methods=["POST"])
def save_prix_ref():
    """Upsert un prix de référence pour un couple chantier+element."""
    try:
        data = request.get_json(force=True, silent=True)
        chantier = data.get("chantier", "").strip()
        element  = data.get("element", "").strip()
        prix     = float(data.get("prix_unitaire", 0))
        if not chantier or not element:
            return jsonify({"erreur": "chantier et element requis"}), 400
        conn = get_conn()
        conn.run(
            """INSERT INTO prix_reference (chantier, element, prix_unitaire, maj_le)
               VALUES (:c, :e, :p, NOW())
               ON CONFLICT (chantier, element)
               DO UPDATE SET prix_unitaire=EXCLUDED.prix_unitaire, maj_le=NOW()""",
            c=chantier, e=element, p=prix
        )
        conn.close()
        return jsonify({"ok": True})
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


# ── Anomalies ─────────────────────────────────────────────────────────────────

SCHEMA_ANOMALIES = """
CREATE TABLE IF NOT EXISTS anomalies (
    id          SERIAL PRIMARY KEY,
    date_ano    DATE NOT NULL DEFAULT CURRENT_DATE,
    nom_machine TEXT NOT NULL,
    num_parc    TEXT NOT NULL DEFAULT '',
    auteur      TEXT NOT NULL,
    description TEXT NOT NULL,
    statut      TEXT NOT NULL DEFAULT 'ouvert',
    cree_le     TIMESTAMPTZ DEFAULT NOW()
)
"""

def init_anomalies():
    conn = get_conn()
    conn.run(SCHEMA_ANOMALIES)
    conn.close()

try:
    init_anomalies()
except Exception as e:
    print(f"AVERTISSEMENT init_anomalies: {e}")


@app.route("/api/anomalies", methods=["GET"])
def get_anomalies():
    try:
        role   = request.args.get("role", "chef")
        auteur = request.args.get("auteur", "")
        conn   = get_conn()
        if role in ("admin", "rh"):
            rows = conn.run(
                "SELECT id,date_ano,nom_machine,num_parc,auteur,description,statut,cree_le "
                "FROM anomalies ORDER BY cree_le DESC LIMIT 200"
            )
        else:
            rows = conn.run(
                "SELECT id,date_ano,nom_machine,num_parc,auteur,description,statut,cree_le "
                "FROM anomalies WHERE auteur ILIKE :a ORDER BY cree_le DESC LIMIT 100",
                a=auteur
            )
        conn.close()
        cols = ["id","date_ano","nom_machine","num_parc","auteur","description","statut","cree_le"]
        result = []
        for row in rows:
            d = dict(zip(cols, row))
            d["date_ano"] = str(d["date_ano"])
            d["cree_le"]  = str(d["cree_le"])
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/anomalies", methods=["POST"])
def save_anomalie():
    try:
        data        = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"erreur": "Données invalides"}), 400
        aid         = data.get("id")
        date_ano    = data.get("date_ano", "")
        nom_machine = data.get("nom_machine", "").strip()
        num_parc    = data.get("num_parc", "").strip()
        auteur      = data.get("auteur", "")
        description = data.get("description", "").strip()
        statut      = data.get("statut", "ouvert")
        if not nom_machine or not description:
            return jsonify({"erreur": "Champs obligatoires manquants"}), 400
        conn = get_conn()
        if aid:
            conn.run(
                "UPDATE anomalies SET statut=:s, description=:d WHERE id=:id",
                s=statut, d=description, id=aid
            )
            new_id = aid
        else:
            rows = conn.run(
                """INSERT INTO anomalies (date_ano,nom_machine,num_parc,auteur,description,statut)
                   VALUES (:da,:nm,:np,:au,:de,:st) RETURNING id""",
                da=date_ano, nm=nom_machine, np=num_parc, au=auteur, de=description, st=statut
            )
            new_id = rows[0][0]
        conn.close()
        return jsonify({"ok": True, "id": new_id})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/anomalies/<int:aid>", methods=["DELETE"])
def delete_anomalie(aid):
    try:
        role   = request.args.get("role", "chef")
        auteur = request.args.get("auteur", "")
        conn   = get_conn()
        if role in ("admin", "rh"):
            conn.run("DELETE FROM anomalies WHERE id=:id", id=aid)
        else:
            conn.run("DELETE FROM anomalies WHERE id=:id AND auteur ILIKE :a", id=aid, a=auteur)
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500


@app.route("/api/anomalies/<int:aid>/statut", methods=["POST"])
def update_statut_anomalie(aid):
    """Admin peut changer le statut : ouvert -> en_cours -> resolu"""
    try:
        data   = request.get_json(force=True, silent=True)
        statut = data.get("statut", "ouvert")
        conn   = get_conn()
        conn.run("UPDATE anomalies SET statut=:s WHERE id=:id", s=statut, id=aid)
        conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"erreur": str(e)}), 500
