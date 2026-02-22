╔══════════════════════════════════════════════════════════════╗
║           FGS App — Déploiement sur Render.com              ║
╚══════════════════════════════════════════════════════════════╝

ÉTAPE 1 — Créez un compte GitHub (gratuit)
  → https://github.com
  → "Sign up" avec votre email

ÉTAPE 2 — Créez un dépôt et uploadez les fichiers
  → "New repository" → Nom : fgs-app → Private (recommandé) → Create
  → "uploading an existing file"
  → Glissez TOUS les fichiers de ce dossier (app.py, app.html, 
    requirements.txt, Procfile, render.yaml)
  → "Commit changes"

ÉTAPE 3 — Créez un compte Render.com (gratuit)
  → https://render.com → "Get Started for Free"
  → Connectez avec votre compte GitHub

ÉTAPE 4 — Déployez
  → "New +" → "Web Service"
  → Sélectionnez votre dépôt "fgs-app"
  → Render lit automatiquement render.yaml
  → Il vous demandera la valeur de la variable "COMPTES"
  → SAISISSEZ VOS COMPTES (voir format ci-dessous)
  → Cliquez "Deploy Web Service"
  → Attendez 3-5 minutes → votre URL apparaît en haut

ÉTAPE 5 — Votre app est en ligne !
  URL : https://fgs-app.onrender.com (ou similaire)
  Accessible depuis n'importe quel téléphone, n'importe où.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FORMAT DE LA VARIABLE "COMPTES" (à saisir dans Render)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  identifiant:motdepasse:Nom Affiché,identifiant2:motdepasse2:Nom 2

Exemple avec 5 comptes :
  admin:MonMotDePasse!,chef:ChefFGS2025:Chef Chantier,meca1:Meca#Lyon:Meca Lyon,meca2:Meca#Gre:Meca Grenoble,resp:Resp2025:Responsable

Pour AJOUTER un compte plus tard :
  → Render → votre service → "Environment" → modifiez la variable COMPTES
  → Ajoutez : ,nouveaucompte:sonmotdepasse:Son Nom
  → Save → le service redémarre automatiquement

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SÉCURITÉ
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Les mots de passe sont stockés sur les serveurs Render (chiffrés)
✓ Ils ne sont JAMAIS visibles dans le code source GitHub
✓ L'app est en HTTPS (cadenas vert dans le navigateur)
✓ Chaque connexion est vérifiée côté serveur

⚠ Mettez des mots de passe solides (majuscule + chiffre + symbole)
⚠ Ne partagez pas l'URL publiquement si vos données sont sensibles

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TARIFS RENDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Plan Free (gratuit) :
  - L'app s'endort après 15 min sans activité
  - Premier chargement : ~30 secondes d'attente
  - OK pour tester

Plan Starter (~7$/mois) :
  - Toujours actif, pas d'endormissement
  - Recommandé pour un usage quotidien
