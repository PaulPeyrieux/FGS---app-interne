#!/usr/bin/env python3
"""
FGS App — Serveur local
========================
Prérequis : pip install flask
Lancement  : python serveur.py
Navigateur : http://localhost:5000
"""

import json, os
from flask import Flask, request, jsonify, send_from_directory

app = Flask(__name__, static_folder=".")
# Sur Render.com : montez un "Persistent Disk" sur /data pour que les données survivent aux redémarrages
# Sans disque persistant, les données sont en mémoire temporaire (/tmp)
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
FICHIER_BD = os.path.join(DATA_DIR, "donnees.json")

def lire():
    if not os.path.exists(FICHIER_BD):
        ecrire(defaut())
    with open(FICHIER_BD, "r", encoding="utf-8") as f:
        return json.load(f)

def ecrire(bd):
    with open(FICHIER_BD, "w", encoding="utf-8") as f:
        json.dump(bd, f, ensure_ascii=False, indent=2)

def defaut():
    return {
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
             "annee":2021,"heures":1620,"hEntretien":1500,"seuil":250,
             "vgp":"2026-12-03","site":"Lyon","serie":"KB-U25-3-XYZ","poids":2500,
             "piecesAssociees":[
                 {"pieceId":"P-001","heuresInstallation":1500,"dateInstallation":"2026-01-10"},
                 {"pieceId":"P-003","heuresInstallation":1200,"dateInstallation":"2025-10-01"},
             ]},
            {"id":"EQ-002","catId":"c4","nom":"Chariot télescopique","modele":"Manitou MT625",
             "annee":2020,"heures":3100,"hEntretien":2900,"seuil":200,
             "vgp":"2026-04-15","site":"Grenoble","piecesAssociees":[]},
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

@app.route("/")
def index():
    return send_from_directory(".", "app.html")

@app.route("/api/auth", methods=["POST"])
def auth():
    """
    Vérifie les identifiants.
    
    Chaque compte est une variable d'environnement séparée sur Render :
      KEY   = identifiant de l'utilisateur  (ex: Admin, p.peyrieux)
      VALUE = mot de passe                  (ex: MonMotDePasse)
    
    Le nom affiché dans l'app sera identique à la clé (identifiant).
    Ajoutez autant de variables que d'utilisateurs dans Render.
    """
    try:
        data = request.get_json(force=True, silent=True)
        if not data:
            return jsonify({"ok": False}), 400
        
        identifiant = data.get("identifiant", "").strip()
        mdp = data.get("mdp", "")
        
        if not identifiant or not mdp:
            return jsonify({"ok": False}), 400
        
        # Cherche une variable d'environnement dont le nom correspond à l'identifiant
        # La comparaison est insensible à la casse (Admin == admin)
        for key, value in os.environ.items():
            if key.lower() == identifiant.lower() and value == mdp:
                return jsonify({"ok": True, "nom": key})
        
        return jsonify({"ok": False})
    except Exception as e:
        return jsonify({"ok": False, "erreur": str(e)}), 500

@app.route("/api/bd", methods=["GET"])
def get_bd():
    return jsonify(lire())

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

if __name__ == "__main__":
    import os
    lire()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
