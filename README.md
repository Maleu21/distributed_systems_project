# 📚 Projet MapReduce Distribué – WordCount & Tri d’Occurrences

## 🧠 Contexte

Ce projet a été réalisé dans le cadre du module **Systèmes Répartis (BGD701)** du Mastère Expert Data IA et MLops de Télécom Paris. Il vise à implémenter un système distribué basé sur le paradigme **MapReduce** pour effectuer un **wordcount** sur des fichiers texte répartis sur plusieurs machines, puis trier les mots selon leurs occurrences.

Ce projet pédagogique permet d'explorer les problématiques de parallélisation, de répartition de charge, de robustesse des systèmes distribués et d’évaluer les performances via la **loi d’Amdahl**.

---

## 📌 Objectifs

- Implémenter un système MapReduce complet :
  - **Wordcount** distribué
  - **Tri des mots** par fréquence
- Automatiser le déploiement sur plusieurs machines virtuelles via SSH
- Analyser la **scalabilité** et les **limitations** des systèmes répartis

---

## ⚙️ Architecture

Le projet s’organise en **deux MapReduce successifs**, déployés via un script `deploy.sh`.

### 🗂 1. Premier MapReduce : WordCount
#### Script : `script.py`

- **Mapping** : Extraction des mots et initialisation des occurrences (`<mot> 1`)
- **Shuffle** : Répartition des mots selon leur hachage MD5
- **Reduce** : Agrégation des occurrences par mot
- **Agrégation finale** : Centralisation des résultats dans un fichier global

---

### 🗂 2. Second MapReduce : Tri par Occurrence
#### Script : `script_f.py`

- **Mapping** : Inversion `(mot, occ)` → `(occ, mot)` pour tri
- **Shuffle** : Répartition équilibrée via hachage SHA-256
- **Reduce** : Tri local par fréquence décroissante et ordre alphabétique
- **Agrégation finale** : Fusion des résultats triés en un fichier final

---

### 🧪 Script de Déploiement : `deploy.sh`

- **Initialisation** : Configuration SSH, nettoyage de l’environnement
- **Préparation** :
  - Détection des machines disponibles
  - Téléchargement et découpage des fichiers sources
- **Distribution** : Répartition équitable des données sur les machines
- **Exécution** :
  - Déploiement et exécution du `script.py` pour le WordCount
  - Puis du `script_f.py` pour le tri

---

## 📊 Performance & Loi d’Amdahl

L’accélération théorique est calculée selon la **loi d’Amdahl** :

```
S(N) = 1 / ((1 - P) + (P / N))
```

- **Observation** : Le gain de performance diminue avec le nombre de machines à cause :
  - du faible pourcentage parallélisable
  - du coût des communications inter-machines
- **Exemples** :
  - 10 machines → Speedup ≈ 1.17
  - 30 machines → Speedup ≈ 1.08

---

## 📁 Structure du projet

```
├── deploy.sh           # Script de déploiement automatisé
├── script.py           # Script MapReduce - WordCount
├── script_f.py         # Script MapReduce - Tri des mots
├── machines.txt        # Liste des machines virtuelles (IP / hostnames)
├── log.txt             # Fichier de logs (temps, erreurs, phases)
└── résultats/          # Répertoire pour les fichiers finaux générés
```

---

## 🚀 Lancer le projet

1. **Configurer** `machines.txt` avec vos machines SSH accessibles
2. **Placer** les fichiers sources à traiter dans le répertoire défini dans `deploy.sh`
3. **Exécuter** le script de déploiement :

```bash
bash deploy.sh
```

4. Les résultats finaux sont disponibles dans le dossier `résultats/`

---

## ✍️ Auteur

- **Maël PACAUD**
- Promotion **MSBGD**
- Module **BGD701 – Systèmes Répartis**

---

## 📜 Licence

Projet académique – Utilisation pédagogique uniquement.
