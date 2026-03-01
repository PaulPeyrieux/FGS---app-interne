#!/usr/bin/env python3
"""
FGS App — Serveur avec base de données PostgreSQL
Utilise pg8000 (compatible Python 3.14+)
"""

import json, os, re, io, base64
import pg8000.native
from flask import Flask, request, jsonify, send_from_directory, send_file

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


@app.route("/api/whoami")
def whoami():
    """Debug: retourne les variables d'env disponibles (sans les valeurs) pour diagnostic"""
    comptes = []
    for key, value in os.environ.items():
        if key.lower() in ('admin', 'rh') or (len(key) < 20 and not key.startswith('_') and not key.startswith('RENDER') and not key.startswith('DATABASE') and not key.startswith('PORT') and not key.startswith('PATH') and not key.startswith('HOME') and not key.startswith('USER') and not key.startswith('PWD') and not key.startswith('LANG') and not key.startswith('LC_') and not key.startswith('PYTHON') and not key.startswith('PIP') and not key.startswith('VIRTUAL')):
            # Déduire le rôle comme auth() le ferait
            id_lower = key.lower()
            role_deduit = "admin" if id_lower == "admin" else ("rh" if id_lower == "rh" else "chef")
            has_colon = ":" in value
            if has_colon:
                last_colon = value.rfind(":")
                role_candidat = value[last_colon+1:].strip().lower()
                if role_candidat in ("chef", "admin", "rh"):
                    role_deduit = role_candidat
            comptes.append({"identifiant": key, "role_deduit": role_deduit, "format": "nouveau" if has_colon else "ancien"})
    return jsonify({"comptes": comptes})

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
            # Format variable Render : "motdepasse:role" (ex: MonMDP:admin, MonMDP:chef)
            # ou "motdepasse" seul (ancien format sans rôle -> chef par défaut)
            # L'utilisateur saisit UNIQUEMENT le mot de passe, sans le :role
            role = "chef"
            mdp_stocke = value
            if ":" in value:
                last_colon = value.rfind(":")
                role_candidat = value[last_colon+1:].strip().lower()
                if role_candidat in ("chef", "admin", "rh"):
                    role = role_candidat
                    mdp_stocke = value[:last_colon]
                # sinon le ":" fait partie du mdp (ex: "Illite@8020") -> mdp_stocke = value entier
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



# ══════════════════════════════════════════════════════════════════════════════
# MOTEUR D'EXPORT XLSX PROFESSIONNEL — FGS Travaux Spéciaux
# Génère des fichiers Excel avec logo, cartouche et mise en forme élaborée
# ══════════════════════════════════════════════════════════════════════════════

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
    from openpyxl.utils import get_column_letter
    from openpyxl.drawing.image import Image as XLImage
    OPENPYXL_OK = True
except ImportError:
    OPENPYXL_OK = False

# ── Palette couleurs FGS ──────────────────────────────────────────────────────
C_ROUGE      = "C0392B"   # Rouge FGS principal
C_ROUGE_L    = "FDECEA"   # Rouge clair (fond alertes)
C_ORANGE_L   = "FEF3DC"   # Orange clair
C_VERT_L     = "EDF7ED"   # Vert clair (OK)
C_BLEU_FOND  = "1A1A2E"   # Bleu nuit (cartouche)
C_BLEU_ENT   = "16213E"   # Bleu entêtes
C_BLEU_ACIER = "2C3E50"   # Bleu acier (entêtes tableau)
C_GRIS_SEP   = "ECF0F1"   # Gris séparateur
C_GRIS_LIGNE = "F8F9FA"   # Gris ligne paire
C_BORDURE    = "CED4DA"   # Gris bordure
C_BLANC      = "FFFFFF"
C_TEXTE      = "1A1A2E"

# ── Helpers de style ──────────────────────────────────────────────────────────
def _S(color=C_BORDURE, style='thin'):
    return Side(style=style, color=color)

def _fill(color):
    return PatternFill(fill_type='solid', fgColor=color)

def _font(sz=10, bold=False, color=C_TEXTE, italic=False, name='Calibri'):
    return Font(name=name, size=sz, bold=bold, color=color, italic=italic)

def _align(h='left', v='center', wrap=False):
    return Alignment(horizontal=h, vertical=v, wrap_text=wrap, indent=0)

def _border(left=True, right=True, top=True, bottom=True, c=C_BORDURE):
    s = _S(c)
    return Border(
        left=s if left else None,
        right=s if right else None,
        top=s if top else None,
        bottom=s if bottom else None
    )

def _set_col_widths(ws, widths):
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

def _date_fr(d_str):
    """Convertit 'YYYY-MM-DD' en 'DD/MM/YYYY'."""
    if not d_str or d_str == '—': return '—'
    try:
        from datetime import date
        return date.fromisoformat(str(d_str)).strftime('%d/%m/%Y')
    except:
        return str(d_str)

# ── Cartouche professionnel FGS ───────────────────────────────────────────────
def _cartouche(ws, titre, sous_titre, auteur, nb_cols):
    """
    Construit le cartouche FGS sur 7 lignes :
    L1 : marge vide
    L2 : fond bleu nuit — Nom société (gauche) + Logo (droite)
    L3 : fond rouge FGS — Titre document
    L4 : fond bleu nuit — Sous-titre / description
    L5 : fond bleu nuit — Date export + Exporté par
    L6 : ligne rouge fine (séparateur décoratif)
    L7 : marge vide avant les données
    Retourne le numéro de la 1ère ligne de données (8).
    """
    from datetime import datetime
    now = datetime.now()
    date_str  = now.strftime("%d/%m/%Y")
    heure_str = now.strftime("%Hh%M")

    hauteurs = {1:4, 2:38, 3:30, 4:20, 5:17, 6:5, 7:6}
    for row, h in hauteurs.items():
        ws.row_dimensions[row].height = h

    # Couleurs de fond par ligne
    fonds = {2: C_BLEU_FOND, 3: C_ROUGE, 4: C_BLEU_FOND, 5: C_BLEU_FOND, 6: C_ROUGE}
    for row, color in fonds.items():
        for col in range(1, nb_cols + 1):
            ws.cell(row=row, column=col).fill = _fill(color)

    # L2 : Société
    c = ws.cell(row=2, column=1, value="FGS TRAVAUX SPÉCIAUX")
    c.font      = _font(sz=18, bold=True, color=C_BLANC)
    c.alignment = _align('left', 'center')

    # L3 : Titre document
    c = ws.cell(row=3, column=1, value=f"  {titre.upper()}")
    c.font      = _font(sz=15, bold=True, color=C_BLANC)
    c.alignment = _align('left', 'center')

    # L4 : Sous-titre
    c = ws.cell(row=4, column=1, value=f"  {sous_titre}")
    c.font      = _font(sz=10, italic=True, color='AABBCC')
    c.alignment = _align('left', 'center')

    # L5 : Exporté par + date
    c = ws.cell(row=5, column=1,
                value=f"  Exporté le {date_str} à {heure_str}   •   Par : {auteur}")
    c.font      = _font(sz=9, color='8899AA')
    c.alignment = _align('left', 'center')

    # Logo (ancré en haut à droite du cartouche)
    logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logo_fgs.png')
    if os.path.exists(logo_path):
        try:
            img = XLImage(logo_path)
            ratio = img.width / max(img.height, 1)
            img.height = 82
            img.width  = int(82 * ratio)
            # Placer dans les 2-3 dernières colonnes
            col_logo = max(nb_cols - 2, 2)
            img.anchor = f"{get_column_letter(col_logo)}2"
            ws.add_image(img)
        except Exception as e:
            pass  # Si le logo échoue, on continue sans

    # Figer les lignes d'en-tête (cartouche + entêtes tableau)
    ws.freeze_panes = "A9"
    return 8  # première ligne disponible (après cartouche)


# ── Ligne d'en-têtes tableau ──────────────────────────────────────────────────
def _entetes(ws, row, cols, hauteur=22):
    ws.row_dimensions[row].height = hauteur
    for i, col in enumerate(cols, 1):
        c = ws.cell(row=row, column=i, value=col)
        c.font      = _font(sz=10, bold=True, color=C_BLANC)
        c.fill      = _fill(C_BLEU_ACIER)
        c.alignment = _align('center', 'center')
        c.border    = Border(
            left   = _S(C_BLEU_ACIER, 'medium'),
            right  = _S(C_BLANC, 'thin'),
            top    = _S(C_BLEU_ACIER, 'medium'),
            bottom = _S(C_BLEU_ACIER, 'medium')
        )

def _entetes_rouge(ws, row, cols, hauteur=22):
    """Variante rouge pour les synthèses financières."""
    ws.row_dimensions[row].height = hauteur
    for i, col in enumerate(cols, 1):
        c = ws.cell(row=row, column=i, value=col)
        c.font      = _font(sz=10, bold=True, color=C_BLANC)
        c.fill      = _fill(C_ROUGE)
        c.alignment = _align('center', 'center')
        c.border    = Border(
            left   = _S(C_ROUGE, 'medium'),
            right  = _S(C_BLANC, 'thin'),
            top    = _S(C_ROUGE, 'medium'),
            bottom = _S(C_ROUGE, 'medium')
        )


# ── Ligne de données ──────────────────────────────────────────────────────────
def _ligne(ws, row, data, idx=0, hauteur=18, surbrillance=None, bold_col1=False):
    ws.row_dimensions[row].height = hauteur
    bg = surbrillance if surbrillance else (C_GRIS_LIGNE if idx % 2 == 0 else C_BLANC)
    for i, val in enumerate(data, 1):
        c = ws.cell(row=row, column=i, value=val)
        c.font      = _font(sz=10, bold=(bold_col1 and i == 1))
        c.fill      = _fill(bg)
        c.alignment = _align('left', 'center', wrap=(i == len(data)))
        c.border    = Border(
            bottom = _S(C_BORDURE, 'thin'),
            right  = _S(C_BORDURE, 'thin')
        )


def _ligne_total(ws, row, data, nb_cols, color=C_BLEU_ACIER, hauteur=22):
    """Ligne de total en pied de tableau."""
    ws.row_dimensions[row].height = hauteur
    for col in range(1, nb_cols + 1):
        ws.cell(row=row, column=col).fill = _fill(color)
    for i, val in enumerate(data, 1):
        if val is None: continue
        c = ws.cell(row=row, column=i, value=val)
        c.font      = _font(sz=11, bold=True, color=C_BLANC)
        c.alignment = _align('left', 'center')


def _titre_section(ws, row, texte, nb_cols, color=C_GRIS_SEP, hauteur=16):
    """Ligne de séparation de section (sous-titre grisé)."""
    ws.row_dimensions[row].height = hauteur
    for col in range(1, nb_cols + 1):
        ws.cell(row=row, column=col).fill = _fill(color)
    c = ws.cell(row=row, column=1, value=f"  {texte}")
    c.font      = _font(sz=10, bold=True, color=C_BLEU_ACIER)
    c.alignment = _align('left', 'center')


# ── Workbook helpers ──────────────────────────────────────────────────────────
def _new_wb():
    wb = Workbook()
    wb.remove(wb.active)  # Supprimer la feuille vide par défaut
    return wb

def _send_wb(wb, filename):
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(
        buf,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f"{filename}.xlsx"
    )


# ── Route principale ──────────────────────────────────────────────────────────
@app.route("/api/export/<type_export>", methods=["POST"])
def export_xlsx(type_export):
    if not OPENPYXL_OK:
        return jsonify({"erreur": "openpyxl non disponible sur ce serveur"}), 500
    try:
        payload  = request.get_json(force=True, silent=True) or {}
        auteur   = payload.get("auteur", "—")
        bd       = payload.get("bd", {})
        data     = payload.get("data", {})
        from datetime import datetime
        date_iso = datetime.now().strftime("%Y-%m-%d")

        handlers = {
            "parc":              lambda: _export_parc(bd, auteur, date_iso),
            "entretiens":        lambda: _export_entretiens(bd, auteur, date_iso),
            "pieces":            lambda: _export_pieces(bd, auteur, date_iso),
            "pointage_jour":     lambda: _export_ptg_jour(data.get("pointages",[]), auteur, date_iso),
            "pointage_semaine":  lambda: _export_ptg_semaine(data.get("pointages",[]), auteur, date_iso),
            "livraisons_admin":  lambda: _export_liv_admin(data, auteur, date_iso),
            "livraisons_chef":   lambda: _export_liv_chef(data, auteur, date_iso),
            "total_chantier":    lambda: _export_total(data, bd, auteur, date_iso),
        }
        if type_export not in handlers:
            return jsonify({"erreur": f"Export inconnu : {type_export}"}), 400
        return handlers[type_export]()
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"erreur": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 1 — PARC MATÉRIEL
# ══════════════════════════════════════════════════════════════════════════════
def _export_parc(bd, auteur, date_iso):
    machines   = bd.get("machines", [])
    categories = bd.get("categories", [])

    def ncat(cid):
        return next((c.get("nom","—") for c in categories if c.get("id") == cid), "—")

    def st_mach(m):
        from datetime import date
        seuil = m.get("seuil", 250) or 250
        d = (m.get("heures",0) or 0) - (m.get("hEntretien",0) or 0)
        pct = d / seuil
        vgp = m.get("vgp","")
        if vgp:
            try:
                days = (date.fromisoformat(vgp) - date.today()).days
                if days < 0:  return "da"
                if days < 30: return "wa"
            except: pass
        if pct >= 1:   return "da"
        if pct >= 0.8: return "wa"
        return "ok"

    ST_LBL  = {"da": "⚠ EN RETARD", "wa": "À PRÉVOIR", "ok": "À jour"}
    ST_FILL = {"da": C_ROUGE_L,      "wa": C_ORANGE_L,  "ok": C_VERT_L}

    COLS     = ["Machine","Catégorie","Modèle","Année",
                "Site / Chantier","Heures","Dernier entretien",
                "Écart","Seuil","Statut","VGP prochaine"]
    LARGEURS = [28, 18, 22, 8, 20, 12, 16, 10, 10, 14, 14]

    wb = _new_wb()
    ws = wb.create_sheet("Parc matériel")
    _set_col_widths(ws, LARGEURS)

    r = _cartouche(ws, "Parc Matériel — Inventaire Engins",
                   f"{len(machines)} engin(s) · Exporté depuis FGS App", auteur, len(COLS))
    _entetes(ws, r, COLS); r += 1

    # Grouper par catégorie
    cat_order = [c.get("nom","—") for c in categories]
    cat_dict = {cn: [] for cn in cat_order}
    cat_dict["Autres"] = []
    for m in machines:
        cn = ncat(m.get("catId",""))
        if cn in cat_dict: cat_dict[cn].append(m)
        else: cat_dict["Autres"].append(m)

    idx = 0
    for cn in cat_order + ["Autres"]:
        ms = cat_dict.get(cn, [])
        if not ms: continue
        _titre_section(ws, r, f"▸  {cn}  —  {len(ms)} engin(s)", len(COLS)); r += 1
        for m in ms:
            st   = st_mach(m)
            d    = (m.get("heures",0) or 0) - (m.get("hEntretien",0) or 0)
            seuil = m.get("seuil",250) or 250
            row_data = [
                m.get("nom","—"),
                cn,
                m.get("modele","—") or "—",
                m.get("annee","—") or "—",
                m.get("site","—") or "—",
                f"{m.get('heures',0)} h",
                f"{m.get('hEntretien',0)} h",
                f"{d} h",
                f"{seuil} h",
                ST_LBL[st],
                _date_fr(m.get("vgp","")) or "—"
            ]
            _ligne(ws, r, row_data, idx, surbrillance=ST_FILL[st]); idx += 1; r += 1

    # Pied : résumé statuts
    r += 1
    nb_da = sum(1 for m in machines if st_mach(m) == "da")
    nb_wa = sum(1 for m in machines if st_mach(m) == "wa")
    nb_ok = sum(1 for m in machines if st_mach(m) == "ok")
    _ligne_total(ws, r, [f"BILAN  :  {nb_da} en retard  •  {nb_wa} à prévoir  •  {nb_ok} à jour",
                          None, None, None, None, None, None, None, None,
                          f"{len(machines)} total", None], len(COLS))

    return _send_wb(wb, f"FGS-Parc-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 2 — ENTRETIENS
# ══════════════════════════════════════════════════════════════════════════════
def _export_entretiens(bd, auteur, date_iso):
    interventions = sorted(bd.get("interventions",[]), key=lambda x: x.get("date",""), reverse=True)
    machines   = bd.get("machines",[])
    pieces     = bd.get("pieces",[])
    categories = bd.get("categories",[])

    def get_m(mid): return next((m for m in machines if m.get("id")==mid), {})
    def get_p(pid): return next((p for p in pieces  if p.get("id")==pid), {})
    def ncat(cid):  return next((c.get("nom","—") for c in categories if c.get("id")==cid), "—")
    TY = {"entretien":"Entretien préventif","reparation":"Réparation",
          "VGP":"VGP","remplacement":"Remplacement pièce"}
    TY_FILL = {"entretien": C_VERT_L, "reparation": C_ROUGE_L,
               "VGP": "E8EAF6", "remplacement": C_ORANGE_L}

    COLS     = ["Date","Machine","Catégorie","Site","Type",
                "Heures","Pièces catalogue","Pièces libres","Notes"]
    LARGEURS = [14, 26, 18, 18, 22, 10, 32, 28, 36]

    wb = _new_wb()
    ws = wb.create_sheet("Entretiens")
    _set_col_widths(ws, LARGEURS)

    r = _cartouche(ws, "Historique des Entretiens",
                   f"{len(interventions)} intervention(s) — Toutes machines", auteur, len(COLS))
    _entetes(ws, r, COLS); r += 1

    for idx, iv in enumerate(interventions):
        m  = get_m(iv.get("machineId",""))
        ty = iv.get("type","entretien")
        pcs_noms = [get_p(pid).get("nom","?") for pid in (iv.get("piecesChangees") or []) if get_p(pid)]
        autres   = [f"{ap.get('nom','?')} ({ap.get('ref','—')})"
                    for ap in (iv.get("autresPieces") or [])]
        row_data = [
            _date_fr(iv.get("date","")),
            m.get("nom","—"),
            ncat(m.get("catId","")),
            m.get("site","—") or "—",
            TY.get(ty, ty),
            f"{iv.get('heures','—')} h" if iv.get("heures") else "—",
            ", ".join(pcs_noms) if pcs_noms else "—",
            ", ".join(autres)   if autres   else "—",
            iv.get("notes","—") or "—"
        ]
        _ligne(ws, r, row_data, idx, surbrillance=TY_FILL.get(ty)); r += 1

    # Légende types
    r += 1
    _titre_section(ws, r, "LÉGENDE", len(COLS)); r += 1
    legende = [
        ("Entretien préventif", C_VERT_L),
        ("Réparation",          C_ROUGE_L),
        ("VGP",                 "E8EAF6"),
        ("Remplacement pièce",  C_ORANGE_L),
    ]
    for i, (lbl, clr) in enumerate(legende):
        _ligne(ws, r, [lbl, "", "", "", "", "", "", "", ""], i, surbrillance=clr); r += 1

    return _send_wb(wb, f"FGS-Entretiens-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 3 — PIÈCES & STOCKS
# ══════════════════════════════════════════════════════════════════════════════
def _export_pieces(bd, auteur, date_iso):
    pieces   = bd.get("pieces",[])
    machines = bd.get("machines",[])

    def get_compat(p):
        ids = p.get("machinesCompatibles") or []
        if not ids:
            ids = [m.get("id") for m in machines
                   if any(pa.get("pieceId")==p.get("id")
                          for pa in (m.get("piecesAssociees") or []))]
        return [m.get("nom","?") for m in machines if m.get("id") in ids]

    COLS     = ["Nom pièce","Référence","Durée de vie","Unité",
                "Stock actuel","Besoin (1/engin)","À commander","Statut","Machines compatibles","Notes"]
    LARGEURS = [26, 16, 14, 10, 14, 14, 14, 18, 40, 30]

    wb = _new_wb()
    ws = wb.create_sheet("Pièces & Stocks")
    _set_col_widths(ws, LARGEURS)

    # Trier : pièces à commander en premier
    def sort_key(p):
        compat = get_compat(p)
        manque = max(len(compat) - (p.get("stock",0) or 0), 0)
        return -manque

    pieces_sorted = sorted(pieces, key=sort_key)
    nb_alerte = sum(1 for p in pieces if max(len(get_compat(p))-(p.get("stock",0) or 0),0)>0)

    r = _cartouche(ws, "Catalogue Pièces & Stocks",
                   f"{len(pieces)} référence(s)  •  {nb_alerte} à commander", auteur, len(COLS))
    _entetes(ws, r, COLS); r += 1

    # Section : pièces à commander
    a_cmd = [p for p in pieces_sorted if max(len(get_compat(p))-(p.get("stock",0) or 0),0)>0]
    ok    = [p for p in pieces_sorted if max(len(get_compat(p))-(p.get("stock",0) or 0),0)==0]

    if a_cmd:
        _titre_section(ws, r, f"⚠  {len(a_cmd)} PIÈCE(S) À COMMANDER", len(COLS), C_ROUGE_L); r += 1
        for idx, p in enumerate(a_cmd):
            compat = get_compat(p)
            stock  = p.get("stock",0) or 0
            manque = max(len(compat) - stock, 0)
            row_data = [p.get("nom","—"), p.get("ref","—"),
                        p.get("dureeVal","—"), p.get("dureeUnite","heures"),
                        stock, len(compat), manque, f"⚠ {manque} à commander",
                        ", ".join(compat) if compat else "—",
                        p.get("notes","—") or "—"]
            _ligne(ws, r, row_data, idx, surbrillance=C_ROUGE_L); r += 1

    if ok:
        _titre_section(ws, r, f"✓  {len(ok)} PIÈCE(S) EN STOCK SUFFISANT", len(COLS), C_VERT_L); r += 1
        for idx, p in enumerate(ok):
            compat = get_compat(p)
            stock  = p.get("stock",0) or 0
            besoin = len(compat)
            statut = "✓ Stock OK" if besoin > 0 else "Non rattachée"
            row_data = [p.get("nom","—"), p.get("ref","—"),
                        p.get("dureeVal","—"), p.get("dureeUnite","heures"),
                        stock, besoin or "—", "—", statut,
                        ", ".join(compat) if compat else "—",
                        p.get("notes","—") or "—"]
            _ligne(ws, r, row_data, idx, surbrillance=C_VERT_L if besoin > 0 else None); r += 1

    # Total
    total_stock = sum(p.get("stock",0) or 0 for p in pieces)
    _ligne_total(ws, r, [f"TOTAL : {len(pieces)} références  •  {total_stock} unités en stock",
                          None,None,None, total_stock, None, None, None, None, None], len(COLS))

    return _send_wb(wb, f"FGS-Pieces-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 4 — POINTAGE JOURNALIER
# ══════════════════════════════════════════════════════════════════════════════
def _export_ptg_jour(pointages, auteur, date_iso):
    from datetime import date
    sorted_ptg = sorted(pointages, key=lambda x: x.get("date_jour",""), reverse=True)
    JOURS = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]

    COLS     = ["Date","Chantier","Saisi par","Employé",
                "Heures","Gd Dépl.","Panier","Notes"]
    LARGEURS = [18, 24, 18, 24, 10, 12, 10, 32]

    wb = _new_wb()
    ws = wb.create_sheet("Pointage journalier")
    _set_col_widths(ws, LARGEURS)

    # Calculs globaux
    total_h_global = sum(float(l.get("heures",0) or 0)
                         for p in sorted_ptg for l in (p.get("lignes") or []))
    nb_gd_global   = sum(1 for p in sorted_ptg for l in (p.get("lignes") or []) if l.get("gd"))
    nb_pan_global  = sum(1 for p in sorted_ptg for l in (p.get("lignes") or []) if l.get("panier"))

    r = _cartouche(ws, "Pointages — Détail par journée",
                   f"{len(sorted_ptg)} journée(s)  •  {total_h_global:.1f} h totales  •  {nb_gd_global} GD  •  {nb_pan_global} paniers",
                   auteur, len(COLS))
    _entetes(ws, r, COLS); r += 1

    for ptg in sorted_ptg:
        dj = ptg.get("date_jour","")
        try:
            d = date.fromisoformat(dj)
            date_lbl = f"{JOURS[d.weekday()]} {d.strftime('%d/%m/%Y')}"
        except:
            date_lbl = dj
        chantier   = ptg.get("chantier","—")
        saisie_par = ptg.get("auteur","—")
        lignes     = ptg.get("lignes") or []
        notes      = ptg.get("notes","") or "—"

        # En-tête de la journée
        _titre_section(ws, r, f"  {date_lbl}  —  {chantier}  —  {len(lignes)} présence(s)", len(COLS)); r += 1

        if not lignes:
            _ligne(ws, r, [date_lbl, chantier, saisie_par, "(aucun employé)",
                           "—","—","—", notes], 0); r += 1
        else:
            for j, l in enumerate(lignes):
                row_data = [
                    date_lbl   if j == 0 else "",
                    chantier   if j == 0 else "",
                    saisie_par if j == 0 else "",
                    l.get("nom","—"),
                    l.get("heures","—") or "—",
                    "✓" if l.get("gd")     else "—",
                    "✓" if l.get("panier") else "—",
                    notes if j == 0 else ""
                ]
                # Fond différent pour GD ou panier
                surb = None
                if l.get("gd") and l.get("panier"): surb = C_ORANGE_L
                elif l.get("gd"):     surb = "FFF3E0"
                elif l.get("panier"): surb = "E8F5E9"
                _ligne(ws, r, row_data, j, surbrillance=surb, bold_col1=(j==0)); r += 1

        # Sous-total journée
        th    = sum(float(l.get("heures",0) or 0) for l in lignes)
        nb_gd = sum(1 for l in lignes if l.get("gd"))
        nb_p  = sum(1 for l in lignes if l.get("panier"))
        ws.row_dimensions[r].height = 15
        c = ws.cell(row=r, column=1,
                    value=f"     Sous-total : {len(lignes)} présence(s)  ·  {th:.1f} h  ·  {nb_gd} GD  ·  {nb_p} panier(s)")
        c.font      = _font(sz=9, italic=True, color=C_BLEU_ACIER, bold=True)
        c.fill      = _fill(C_GRIS_SEP)
        c.alignment = _align('left','center')
        for col in range(2, len(COLS)+1):
            ws.cell(row=r, column=col).fill = _fill(C_GRIS_SEP)
        r += 1
        ws.row_dimensions[r].height = 4; r += 1  # espace entre journées

    # Total global
    _ligne_total(ws, r, [
        f"TOTAL GÉNÉRAL : {len(sorted_ptg)} journée(s)  ·  {total_h_global:.1f} h  ·  {nb_gd_global} GD  ·  {nb_pan_global} panier(s)",
        None, None, None, f"{total_h_global:.1f} h",
        str(nb_gd_global), str(nb_pan_global), None
    ], len(COLS))

    return _send_wb(wb, f"FGS-Pointage-Jour-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 5 — POINTAGE HEBDOMADAIRE (une feuille par semaine)
# ══════════════════════════════════════════════════════════════════════════════
def _export_ptg_semaine(pointages, auteur, date_iso):
    from datetime import date, timedelta

    # Grouper par semaine (lundi)
    sem_map = {}
    for ptg in pointages:
        dj = ptg.get("date_jour","")
        try:
            d    = date.fromisoformat(dj)
            lun  = d - timedelta(days=d.weekday())
            wk   = lun.isoformat()
        except:
            wk = dj[:7]
        if wk not in sem_map: sem_map[wk] = []
        sem_map[wk].append(ptg)

    JOURS    = ["Lun","Mar","Mer","Jeu","Ven","Sam","Dim"]
    COLS     = ["Employé","Chantier"] + JOURS + ["Total h","GD","Paniers"]
    LARGEURS = [26, 24, 8, 8, 8, 8, 8, 8, 8, 12, 10, 10]

    wb = _new_wb()

    for wk in sorted(sem_map.keys(), reverse=True):
        ptgs = sem_map[wk]
        try:
            lun = date.fromisoformat(wk)
            dim = lun + timedelta(days=6)
            lbl = f"{lun.strftime('%d/%m')} — {dim.strftime('%d/%m/%Y')}"
            nom_feuille = f"Sem {lun.strftime('%d.%m.%y')}"[:31]
        except:
            lbl = wk; nom_feuille = wk[:31]

        ws = wb.create_sheet(nom_feuille)
        _set_col_widths(ws, LARGEURS)

        nb_jours = len(ptgs)
        total_h  = sum(float(l.get("heures",0) or 0) for p in ptgs for l in (p.get("lignes") or []))

        r = _cartouche(ws, "Pointages — Récapitulatif Hebdomadaire",
                       f"Semaine du {lbl}  •  {nb_jours} journée(s)  •  {total_h:.1f} h totales",
                       auteur, len(COLS))
        _entetes(ws, r, COLS); r += 1

        # Construire matrice employé × chantier × jours
        emp_map = {}
        for ptg in ptgs:
            dj = ptg.get("date_jour","")
            try: dow = date.fromisoformat(dj).weekday()
            except: dow = 0
            for l in (ptg.get("lignes") or []):
                k = f"{l.get('nom','?')}||{ptg.get('chantier','?')}"
                if k not in emp_map:
                    emp_map[k] = {"nom": l.get("nom","?"),
                                  "chantier": ptg.get("chantier","?"),
                                  "h": [0.0]*7, "gd": 0, "pan": 0}
                emp_map[k]["h"][dow] += float(l.get("heures",0) or 0)
                if l.get("gd"):     emp_map[k]["gd"]  += 1
                if l.get("panier"): emp_map[k]["pan"] += 1

        # Grouper par chantier
        chantiers = sorted(set(v["chantier"] for v in emp_map.values()))
        idx = 0
        for ch in chantiers:
            emps = [v for v in emp_map.values() if v["chantier"] == ch]
            _titre_section(ws, r, f"▸ {ch}", len(COLS)); r += 1
            for e in sorted(emps, key=lambda x: x["nom"]):
                tot  = sum(e["h"])
                row_data = ([e["nom"], e["chantier"]] +
                           [round(h,1) if h > 0 else "" for h in e["h"]] +
                           [round(tot,1), e["gd"] or "", e["pan"] or ""])
                # Fond orange si GD ou panier
                surb = C_ORANGE_L if (e["gd"] or e["pan"]) else None
                _ligne(ws, r, row_data, idx, surbrillance=surb); idx += 1; r += 1

        # Ligne totaux semaine
        h_jours = [sum(v["h"][i] for v in emp_map.values()) for i in range(7)]
        total_sem = sum(h_jours)
        tot_gd  = sum(v["gd"]  for v in emp_map.values())
        tot_pan = sum(v["pan"] for v in emp_map.values())
        _ligne_total(ws, r, (
            [f"TOTAL SEMAINE : {total_sem:.1f} h", ""] +
            [round(h,1) if h > 0 else "" for h in h_jours] +
            [round(total_sem,1), tot_gd or "", tot_pan or ""]
        ), len(COLS)); r += 1

    return _send_wb(wb, f"FGS-Pointage-Semaine-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 6 — LIVRAISONS ADMIN (2 feuilles : récap + détail)
# ══════════════════════════════════════════════════════════════════════════════
def _export_liv_admin(data, auteur, date_iso):
    livraisons = data.get("livraisons", [])
    chantier   = data.get("chantier", "Tous chantiers") or "Tous chantiers"
    filtre_sem = data.get("semaine", "")

    if not livraisons:
        return jsonify({"erreur": "Aucune livraison à exporter"}), 400

    wb = _new_wb()

    # ── Feuille 1 : Récapitulatif ──────────────────────────────────────────────
    COLS_R   = ["Matériau","Quantité totale","Unité","Nb livraisons","Prix unitaire","Coût total HT"]
    LARG_R   = [32, 18, 10, 14, 18, 20]
    ws1 = wb.create_sheet("Récapitulatif")
    _set_col_widths(ws1, LARG_R)

    sous_t = chantier + (f"  —  Semaine {filtre_sem}" if filtre_sem else "")
    r = _cartouche(ws1, "Livraisons Chantier — Récapitulatif", sous_t, auteur, len(COLS_R))
    _entetes_rouge(ws1, r, COLS_R); r += 1

    # Agréger par matériau
    agg = {}
    for l in livraisons:
        el = l.get("element","?")
        if el not in agg:
            agg[el] = {"element":el,"unite":l.get("unite",""),"qte":0.0,"nb":0,"prix":None}
        agg[el]["qte"] += float(l.get("quantite",0) or 0)
        agg[el]["nb"]  += 1
        if l.get("prix_unitaire") and not agg[el]["prix"]:
            agg[el]["prix"] = float(l["prix_unitaire"])

    total_cout = 0.0
    lignes_sorted = sorted(agg.values(), key=lambda x: x["qte"], reverse=True)
    for idx, v in enumerate(lignes_sorted):
        prix = v.get("prix")
        cout = round(v["qte"] * prix, 2) if prix else None
        if cout: total_cout += cout
        row_data = [
            v["element"],
            round(v["qte"], 3),
            v["unite"],
            v["nb"],
            f"{prix:.2f} €/{v['unite']}" if prix else "—",
            f"{cout:.2f} €"               if cout else "—"
        ]
        _ligne(ws1, r, row_data, idx); r += 1

    # Total
    _ligne_total(ws1, r, [
        f"TOTAL — {len(livraisons)} livraison(s)",
        None, None, len(livraisons), None,
        f"{total_cout:.2f} €" if total_cout else "—"
    ], len(COLS_R), color=C_ROUGE)

    # ── Feuille 2 : Détail livraisons ─────────────────────────────────────────
    COLS_D = ["Date","Chantier","Chef chantier",
              "Matériau","Quantité","Unité","Prix unit.","Coût HT","Notes"]
    LARG_D = [14, 22, 18, 26, 12, 10, 16, 16, 30]

    ws2 = wb.create_sheet("Détail livraisons")
    _set_col_widths(ws2, LARG_D)
    r = _cartouche(ws2, "Livraisons Chantier — Détail complet", sous_t, auteur, len(COLS_D))
    _entetes(ws2, r, COLS_D); r += 1

    livs_sort = sorted(livraisons, key=lambda x: x.get("date_liv",""), reverse=True)
    for idx, l in enumerate(livs_sort):
        prix = l.get("prix_unitaire")
        if prix: prix = float(prix)
        cout = round(float(l.get("quantite",0) or 0) * prix, 2) if prix else None
        row_data = [
            _date_fr(l.get("date_liv","")),
            l.get("chantier","—"),
            l.get("auteur","—"),
            l.get("element","—"),
            round(float(l.get("quantite",0) or 0), 3),
            l.get("unite","—"),
            f"{prix:.2f} €" if prix else "—",
            f"{cout:.2f} €" if cout else "—",
            l.get("notes","—") or "—"
        ]
        _ligne(ws2, r, row_data, idx); r += 1

    ws1.freeze_panes = ws2.freeze_panes = "A9"
    nom = chantier.replace(" ","-").replace("/","-")[:20]
    return _send_wb(wb, f"FGS-Livraisons-{nom}-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 7 — LIVRAISONS CHEF (vue simplifiée)
# ══════════════════════════════════════════════════════════════════════════════
def _export_liv_chef(data, auteur, date_iso):
    livraisons = data.get("livraisons", [])
    if not livraisons:
        return jsonify({"erreur": "Aucune livraison à exporter"}), 400

    COLS     = ["Date","Chantier","Matériau","Quantité","Unité","Notes"]
    LARGEURS = [14, 24, 28, 12, 10, 36]

    wb = _new_wb()
    ws = wb.create_sheet("Mes livraisons")
    _set_col_widths(ws, LARGEURS)

    r = _cartouche(ws, "Mes Livraisons Chantier",
                   f"{auteur}  •  {len(livraisons)} livraison(s)", auteur, len(COLS))
    _entetes(ws, r, COLS); r += 1

    for idx, l in enumerate(sorted(livraisons, key=lambda x: x.get("date_liv",""), reverse=True)):
        row_data = [
            _date_fr(l.get("date_liv","")),
            l.get("chantier","—"),
            l.get("element","—"),
            round(float(l.get("quantite",0) or 0), 3),
            l.get("unite","—"),
            l.get("notes","—") or "—"
        ]
        _ligne(ws, r, row_data, idx); r += 1

    total_items = len(livraisons)
    _ligne_total(ws, r, [f"TOTAL : {total_items} livraison(s)", None, None, None, None, None], len(COLS))

    return _send_wb(wb, f"FGS-MesLivraisons-{date_iso}")


# ══════════════════════════════════════════════════════════════════════════════
# EXPORT 8 — TOTAL CHANTIER (synthèse complète sur 3 feuilles)
# ══════════════════════════════════════════════════════════════════════════════
def _export_total(data, bd, auteur, date_iso):
    chantier   = data.get("chantier","—")
    pointages  = data.get("pointages",[])
    livraisons = data.get("livraisons",[])
    prix_ref   = data.get("prix_ref",{})

    # ── Calculs MO ────────────────────────────────────────────────────────────
    total_h  = sum(float(l.get("heures",0) or 0)
                   for p in pointages for l in (p.get("lignes") or []))
    nb_pres  = sum(len(p.get("lignes") or []) for p in pointages)
    nb_gd    = sum(1 for p in pointages for l in (p.get("lignes") or []) if l.get("gd"))
    nb_pan   = sum(1 for p in pointages for l in (p.get("lignes") or []) if l.get("panier"))
    taux_h   = prix_ref.get("__heures__")
    cout_mo  = round(total_h * float(taux_h), 2) if taux_h else None

    # ── Calculs matériaux ─────────────────────────────────────────────────────
    agg_mat = {}
    for l in livraisons:
        el = l.get("element","?")
        if el not in agg_mat:
            agg_mat[el] = {"qte":0.0,"unite":l.get("unite",""),"prix":None}
        agg_mat[el]["qte"] += float(l.get("quantite",0) or 0)
        if l.get("prix_unitaire") and not agg_mat[el]["prix"]:
            agg_mat[el]["prix"] = float(l["prix_unitaire"])

    total_mat = 0.0
    mat_lignes = []
    for el, v in agg_mat.items():
        # Chercher prix dans prix_ref en priorité
        prix = float(prix_ref[el]) if el in prix_ref else v.get("prix")
        cout = round(v["qte"] * prix, 2) if prix else None
        if cout: total_mat += cout
        mat_lignes.append((el, round(v["qte"],3), v["unite"], prix, cout))

    total_gen = (cout_mo or 0) + total_mat

    wb = _new_wb()

    # ── Feuille 1 : SYNTHÈSE ──────────────────────────────────────────────────
    COLS_S   = ["Poste","Détail","Taux / Unité","Montant HT"]
    LARG_S   = [22, 50, 20, 22]
    ws1 = wb.create_sheet("Synthèse")
    _set_col_widths(ws1, LARG_S)

    r = _cartouche(ws1, f"Synthèse Chantier — {chantier}",
                   "Main d'œuvre & Matériaux & Coût total", auteur, len(COLS_S))
    _entetes_rouge(ws1, r, COLS_S); r += 1

    _ligne(ws1, r, [
        "Main d'œuvre",
        f"{total_h:.1f} h  ·  {nb_pres} présence(s)  ·  {nb_gd} GD  ·  {nb_pan} panier(s)",
        f"{taux_h} €/h" if taux_h else "Taux non défini",
        f"{cout_mo:.2f} €" if cout_mo else "—"
    ], 0, surbrillance="EAF0FB", bold_col1=True); r += 1

    _ligne(ws1, r, [
        "Matériaux",
        ", ".join(agg_mat.keys()) or "—",
        "Voir feuille Matériaux",
        f"{total_mat:.2f} €" if total_mat else "—"
    ], 1, surbrillance=C_VERT_L, bold_col1=True); r += 1

    # Total général — ligne en rouge
    ws1.row_dimensions[r].height = 26
    for col in range(1, len(COLS_S)+1):
        ws1.cell(row=r, column=col).fill = _fill(C_ROUGE)
    ws1.cell(row=r, column=1, value="TOTAL GÉNÉRAL").font  = _font(sz=13, bold=True, color=C_BLANC)
    ws1.cell(row=r, column=1).alignment = _align('left','center')
    c = ws1.cell(row=r, column=4, value=f"{total_gen:.2f} €" if total_gen else "—")
    c.font = _font(sz=14, bold=True, color=C_BLANC); c.alignment = _align('right','center')

    # ── Feuille 2 : MATÉRIAUX ─────────────────────────────────────────────────
    COLS_M   = ["Matériau","Quantité","Unité","Prix unitaire HT","Coût total HT"]
    LARG_M   = [32, 16, 12, 20, 20]
    ws2 = wb.create_sheet("Matériaux")
    _set_col_widths(ws2, LARG_M)

    r = _cartouche(ws2, f"Matériaux Livrés — {chantier}",
                   f"{len(mat_lignes)} référence(s)  •  {len(livraisons)} livraison(s)", auteur, len(COLS_M))
    _entetes(ws2, r, COLS_M); r += 1

    for idx, (el, qte, unite, prix, cout) in enumerate(sorted(mat_lignes, key=lambda x: -(x[4] or 0))):
        _ligne(ws2, r, [el, qte, unite,
                        f"{prix:.2f} €/{unite}" if prix else "—",
                        f"{cout:.2f} €" if cout else "—"], idx); r += 1

    _ligne_total(ws2, r, [
        f"TOTAL  —  {len(mat_lignes)} matériau(x)", None, None, None,
        f"{total_mat:.2f} €" if total_mat else "—"
    ], len(COLS_M), color=C_ROUGE)

    # ── Feuille 3 : POINTAGES ─────────────────────────────────────────────────
    COLS_P   = ["Date","Saisi par","Employés présents","Présences","Heures totales","GD","Paniers"]
    LARG_P   = [14, 18, 40, 12, 14, 10, 10]
    ws3 = wb.create_sheet("Pointages")
    _set_col_widths(ws3, LARG_P)

    r = _cartouche(ws3, f"Pointages — {chantier}",
                   f"{len(pointages)} journée(s) saisie(s)  •  {total_h:.1f} h totales", auteur, len(COLS_P))
    _entetes(ws3, r, COLS_P); r += 1

    for idx, ptg in enumerate(sorted(pointages, key=lambda x: x.get("date_jour",""), reverse=True)):
        lignes = ptg.get("lignes") or []
        th  = sum(float(l.get("heures",0) or 0) for l in lignes)
        ngd = sum(1 for l in lignes if l.get("gd"))
        npa = sum(1 for l in lignes if l.get("panier"))
        row_data = [
            _date_fr(ptg.get("date_jour","")),
            ptg.get("auteur","—"),
            ", ".join(l.get("nom","?") for l in lignes) or "—",
            len(lignes),
            round(th, 1),
            ngd or "—",
            npa or "—"
        ]
        _ligne(ws3, r, row_data, idx); r += 1

    _ligne_total(ws3, r, [
        f"TOTAL : {len(pointages)} journée(s)", None, None,
        nb_pres, round(total_h,1), nb_gd or "—", nb_pan or "—"
    ], len(COLS_P))

    ws1.freeze_panes = ws2.freeze_panes = ws3.freeze_panes = "A9"
    nom = chantier.replace(" ","-").replace("/","-")[:20]
    return _send_wb(wb, f"FGS-Total-{nom}-{date_iso}")
