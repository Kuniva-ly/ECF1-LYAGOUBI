#!/ RGPD - Conformite

Ce document decrit la conformite RGPD pour le projet DataPulse Multi-Sources.

## **1 Inventaire des donnees personnelles**

Sources :
- Fichier partenaire `partenaire_librairies.xlsx` :
  - contact_nom (donnee personnelle)
  - contact_email (donnee personnelle)
  - contact_telephone (donnee personnelle)

Traitement :
- Les champs personnels ne sont pas stockes en clair.
- Ils sont pseudonymises via hash SHA-256 :
  - contact_nom_hash
  - contact_email_hash
  - contact_telephone_hash

## **2 Base legale**

Base legale retenue : interet legitime (gestion de la relation partenaires).
Les donnees personnelles sont limitees au strict necessaire.

## **3 Mesures de protection**

- Minimisation : seules les donnees utiles sont traitees.
- Pseudonymisation : hash SHA-256 des champs personnels.
- Separation des donnees : les donnees techniques et analytiques sont stockees
  dans PostgreSQL, les exports sont dans MinIO.
- Acces restreint : identifiants limites aux environnements de travail.

## **4 Procedure de suppression (droit a l'effacement)**

1. Identifier la ligne partenaire concernee (par nom/adresse).
2. Supprimer la ligne dans PostgreSQL (table `partners`).
3. Supprimer les exports associes si necessaire (bucket MinIO `data-exports`).
4. Journaliser la demande de suppression.

## **5) Conservation**

Les donnees sont conservees uniquement pour la duree necessaire aux analyses.
Une purge peut etre declenchee sur demande.
