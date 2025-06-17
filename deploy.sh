#!/bin/bash

# Début de l'exécution du script et initialisation des variables

temps_debut_execution=$(date +%s) # Temps de démarrage de l'exécution
log="log.txt" # Fichier de log pour les messages d'exécution

# Fonction pour calculer la durée et afficher le temps écoulé
function temps_execution {
    local temps_debut_phase=$1 # Heure de début de la phase
    local nom_phase=$2 # Nom de la phase en cours
    local temps_fin_phase=$(date +%s) # Heure de fin de la phase
    local duree_phase=$((temps_fin_phase - temps_debut_phase)) # Calcul de la durée de la phase
    echo "$nom_phase : $duree_phase secondes" >> $log # Enregistrement du temps écoulé dans le log
}

# Définition des chemins et des variables spécifiques à l'exécution
login="mpacaud-24" # Nom d'utilisateur pour l'accès
localFolder="./dossierAdeployer" # Répertoire local où se trouve le dossier de déploiement
remoteFolder="/home/users/mpacaud-24/mpacaud" # Répertoire distant de destination
nameOfTheScript="script.py" # Nom du script à exécuter
dossierSource="/cal/commoncrawl" # Dossier contenant les fichiers source
taillePartie=64M # Taille des parties fractionnées
max_machines=6 # Nombre maximal de machines à utiliser

# Définir les variables pour le second MapReduce
nameOfTheScriptSort="script_f.py"
sortInputFile="$localFolder/resultat.txt"  # Résultat final du premier MapReduce
sortOutputFolder="$remoteFolder/tri"  # Dossier pour les fichiers triés sur les machines
finalSortedFile="$localFolder/resultatTrie.txt"  # Fichier trié final

# Phase 0 : Nettoyage des fichiers résiduels des précédentes exécutions
rm -f "$localFolder"/partie* "$localFolder/fichiers_sources.txt" "$localFolder/resultat.txt" "$localFolder"/*.warc.wet "$localFolder/resultatTrie.txt"

# Initialiser le log
echo "Lancement du déploiement avec un objectif de $max_machines machines" > $log

# Phase 1 : Test des connexions SSH
echo "Validation des connexions SSH..." >> $log
machinesDisponibles=()
for ip_machine in $(cat machines.txt); do
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$login@$ip_machine" "echo 'Connexion établie' > /dev/null 2>&1"; then
        machinesDisponibles+=("$ip_machine")
        echo "Succès de la connexion : $ip_machine" >> $log
        if [ "${#machinesDisponibles[@]}" -ge "$max_machines" ]; then
            break
        fi
    else
        echo "Echec de connexion : $ip_machine" >> $log
    fi
done

# Calculer le nombre de machines accessibles
n_machines=${#machinesDisponibles[@]}

# Vérifier si le nombre de machines accessibles est suffisant
if [ "$n_machines" -lt "$max_machines" ]; then
    echo "Machines disponibles : $n_machines (moins que les $max_machines nécessaires)." >> $log
fi

# Arrêter le script si aucune machine n'est accessible
if [ "$n_machines" -eq 0 ]; then
    echo "Aucune machine accessible. Fin du processus." >> $log
    exit 1
fi

# Afficher les machines retenues
echo "Machines retenues ($n_machines disponibles) : ${machinesDisponibles[*]}" >> $log

# Phase 2 : Récupération des fichiers sources
# Créer le répertoire local pour stocker les fichiers téléchargés
mkdir -p "$localFolder"

# Lister les fichiers dans le dossier source sur la première machine disponible
ssh "$login@${machinesDisponibles[0]}" "ls $dossierSource" > "$localFolder/fichiers_sources.txt"

# Lecture des premiers fichiers listés
fichiersAtelecharger=$(head -n 1 "$localFolder/fichiers_sources.txt")
tailleTotale=0

# Début du téléchargement des fichiers
temps_debut_phase=$(date +%s)
for fichier in $fichiersAtelecharger; do
    echo "Tentative de téléchargement du fichier : $fichier" >> $log
    if scp "$login@${machinesDisponibles[0]}:$dossierSource/$fichier" "$localFolder/"; then
        echo "Téléchargement réussi : $fichier" >> $log
        tailleDeFichier=$(stat -c%s "$localFolder/$fichier")
        tailleTotale=$((tailleTotale + tailleDeFichier))
    else
        echo "Erreur : Échec du téléchargement de $file. Passons au fichier suivant." >> $log
        continue
    fi
done
temps_execution $temps_debut_phase "Téléchargement des fichiers"

# Ajouter le poids total des fichiers téléchargés au log
echo "Taille totale des fichiers téléchargés : $((tailleTotale / 1024 / 1024)) Mo" >> $log

# Phase 3 : Fractionnement des fichiers
temps_debut_phase=$(date +%s)
compteur_parties=1

# Parcourir les fichiers du dossier local
for fichier in "$localFolder"/*; do
    if [[ -f "$fichier" && "$fichier" != "$localFolder/fichiers_sources.txt" && "$fichier" != "$localFolder/script.py" && "$fichier" != "$localFolder/script_f.py" ]]; then
        
        # Fractionner le fichier s'il est valide et n'est pas un fichier à exclure
        echo "Fractionnement du fichier : $fichier" >> $log
        split -b $taillePartie "$fichier" "$localFolder/tmp_partie_"

        # Renommer chaque fichier fractionné
        for partie in "$localFolder"/tmp_partie_*; do
            mv "$partie" "$localFolder/partie${compteur_parties}"
            compteur_parties=$((compteur_parties + 1))
        done
    else

        # Gérer les fichiers exclus du fractionnement
        if [[ "$fichier" == "$localFolder/fichiers_sources.txt" || "$fichier" == "$localFolder/script.py" || "$fichier" == "$localFolder/script_f.py" ]]; then
            echo "Fichier ignoré pour le fractionnement : $fichier" >> $log
        else
            echo "Fichier $fichier introuvable ou inaccessible pour le fractionnement." >> $log
        fi
    fi
done

# Enregistrer la durée de la phase de fractionnement
temps_execution $temps_debut_phase "Fractionnement des fichiers"


# Phase 4 : Distribution des fractions sur les machines
temps_debut_phase=$(date +%s)

# Créer le répertoire distant sur chaque machine
for ip_machine in "${machinesDisponibles[@]}"; do
    ssh "$login@$ip_machine" "rm -rf $remoteFolder && mkdir -p $remoteFolder"
    ssh "$login@$ip_machine" "rm -rf $sortOutputFolder  && mkdir -p $sortOutputFolder"
done

# Initialiser un compteur pour gérer les fractions
i=1

# Distribuer chaque fraction sur une machine différente
for partie in "$localFolder"/partie*; do
    targetMachine=${machinesDisponibles[$(( (i - 1) % n_machines ))]} # Utilise n_machines
    scp "$partie" "$login@$targetMachine:$remoteFolder/"
    i=$((i + 1))
done

# Enregistrer la durée de la phase de distribution des fractions
temps_execution $temps_debut_phase "Répartition des blocs"

# PHASE DE MAPPING
echo "Début Phase 1 : Mapping" >> $log

temps_debut_phase=$(date +%s)
i=1

# Lancer le mapping en parallèle sur chaque machine disponible
for ip_machine in "${machinesDisponibles[@]}"; do
    (
        echo "Lancement du mapping sur la machine $ip_machine"
        
        # Copier le script sur la machine distante
        scp "$localFolder/$nameOfTheScript" "$login@$ip_machine:$remoteFolder/"
        
        # Déployer le script_f.py sur toutes les machines
        echo "Déploiement de script_f.py sur les machines..." >> $log
        scp "$localFolder/$nameOfTheScriptSort" "$login@$ip_machine:$remoteFolder/"

       
       # Tentatives de mapping
        tentative=1
        max_tentatives=5
        reussi=0

        while [ $tentative -le $max_tentatives ]; do

            # Vérifier la présence des blocs à traiter
            parties=$(ssh "$login@$ip_machine" "ls $remoteFolder/partie* 2>/dev/null")
            if [ -z "$parties" ]; then
                echo "Aucun fichier trouvé sur $ip_machine après $tentative tentative(s)"
                break
            fi

            # Traiter chaque fraction trouvée
            for partie in $parties; do
                ssh "$login@$ip_machine" "cd $remoteFolder && python3 \"$nameOfTheScript\" mapping \"$i\" \"$partie\" \"$remoteFolder/mapping_${i}_${partie##*/}.txt\""
                
                # Vérification du succès du mapping
                if [ $? -ne 0 ]; then
                    echo "Échec du mapping pour $partie sur $ip_machine"
                    reussi=0
                    break
                else
                    reussi=1
                    echo "Mapping terminé pour $partie sur $ip_machine"
                fi
            done

            # Si le mapping est réussi, sortir de la boucle de tentatives
            if [ $reussi -eq 1 ]; then
                break
            fi

            # Sinon, réessayer après un délai progressif
            tentative=$((tentative + 1))
            sleep $((tentative * 2))
        done

        # Finalisation du processus de mapping
        if [ $reussi -ne 1 ]; then
            echo "Échec complet du mapping sur la machine $ip_machine après $max_tentatives tentatives"
        else
            echo "OK Phase 1 : Mapping Machine $ip_machine" >> $log
        fi
    ) &

    # Incrémenter l'index de la machine pour passer à la fraction suivante
    i=$((i + 1))
done

# Attendre que tous les processus de mapping soient terminés
wait

# Enregistrer le temps d'exécution de la phase de mapping
temps_execution $temps_debut_phase "Phase de mapping"

# Affichage de la fin complète de la phase de mapping
echo "Fin Phase 1 : Mapping" >> $log

# PHASE DE SHUFFLE
temps_debut_phase=$(date +%s)

# Affichage du début de la phase de shuffle
echo "Début Phase 2 : Shuffle" >> $log

# Initialisation du compteur de fractions
i=1

# Lancement du shuffle en parallèle sur chaque machine distante
for ip_machine in "${machinesDisponibles[@]}"; do
    {
        tentative=1
        max_tentatives=5

         # Essayer la phase de shuffle plusieurs fois en cas d'échec
        while [ $tentative -le $max_tentatives ]; do
            
            # Lancer la phase de shuffle sur la machine distante
           ssh "$login@$ip_machine" "cd $remoteFolder && python3 $nameOfTheScript shuffle $i $remoteFolder $remoteFolder $n_machines"
            
            # Vérifier si la phase de shuffle a réussi
            if [ $? -eq 0 ]; then
                echo " OK Phase 2 : Shuffle Machine $ip_machine pour partie $i"
                break
            fi

            # En cas d'échec, afficher un message d'erreur et réessayer après un délai
            echo "Erreur dans la phase de shuffle sur la machine $ip_machine pour partie $i, tentative $tentative/$max_tentatives"
            tentative=$((tentative + 1))
            sleep $((tentative * 2))
        done

        # Si après plusieurs tentatives la phase échoue, l'enregistrer dans le log
        if [ $tentative -gt $max_tentatives ]; then
            echo "Échec complet de la phase de shuffle sur la machine $ip_machine pour partie $i"
        fi
    } &

    # Incrémenter l'index de la fraction pour passer à la suivante
    i=$((i + 1))
done

# Attendre que tous les processus de shuffle soient terminés
wait

# Enregistrer le temps d'exécution de la phase de shuffle
temps_execution $temps_debut_phase "Phase de shuffle"

# Affichage de la fin complète de la phase de shuffle
echo "Fin Phase 2 : Shuffle" >> $log

# PHASE DE REDUCE
temps_debut_phase=$(date +%s)

# Initialisation du compteur de fractions
i=1

# Affichage du début de la phase de réduction
echo "Début Phase 3 : Réduction" >> $log

# Lancement de la phase de réduction en parallèle sur chaque machine distante
for ip_machine in "${machinesDisponibles[@]}"; do
    {
        # Combiner les fichiers de shuffle pour créer les entrées de réduction
        ssh "$login@$ip_machine" "cd $remoteFolder && cat shuffle_${i}_from_machine_*.txt > reduce_input_${i}.txt"
        
        tentative=1
        max_tentatives=5

        # Essayer plusieurs fois en cas d'échec
        while [ $tentative -le $max_tentatives ]; do

            # Exécuter le script de réduction sur la machine distante
            ssh "$login@$ip_machine" "cd $remoteFolder && python3 $nameOfTheScript reduce $i $remoteFolder/reduce_input_${i}.txt $remoteFolder/reduce_machine${i}.txt"
           
           # Vérifier si la réduction a réussi
           if [ $? -eq 0 ]; then
                echo "OK Phase 3 : Réduction Machine $ip_machine pour partie $i" >> $log
                break
            fi

            # En cas d'échec, afficher un message d'erreur et réessayer après un délai
            echo "Erreur dans la phase de réduction sur la machine $ip_machine pour partie $i, tentative $tentative/$max_tentatives"
            tentative=$((tentative + 1))
            sleep $((tentative * 2))
        done

        # Si la réduction échoue après toutes les tentatives, l'enregistrer dans le log
        if [ $tentative -gt $max_tentatives ]; then
            echo "Échec complet de la phase de réduction sur la machine $ip_machine pour partie $i"
        fi
    } &

    # Incrémenter l'index des parties pour passer à la suivante
    i=$((i + 1))
done

# Attendre la fin de tous les processus de réduction
wait

# Enregistrer le temps d'exécution de la phase de réduction
temps_execution $temps_debut_phase "Phase de réduction"

# Affichage de la fin complète de la phase de réduction
echo "Fin Phase 3 : Réduction" >> $log

# PHASE D'AGRÉGATION
temps_debut_phase=$(date +%s)


# Affichage du début de la phase d'agrégation
echo "Début Phase 4 : Agrégation" >> $log

# Définir le fichier de résultat
fichierResultat="$localFolder/resultat.txt"

# Créer le fichier de résultat
touch "$fichierResultat"

# Récupération des fichiers reduce depuis les machines distantes
echo "Récupération des fichiers reduce depuis les machines distantes..." >> $log
i=1
for ip_machine in "${machinesDisponibles[@]}"; do
    tentative=1
    max_tentatives=5

    # Essayer plusieurs fois en cas d'échec
    while [ $tentative -le $max_tentatives ]; do

        # Copier le fichier reduce depuis la machine distante
        scp "$login@$ip_machine:$remoteFolder/reduce_machine${i}.txt" "$localFolder/"
        
        # Vérifier si le fichier a été récupéré avec succès
        if [ $? -eq 0 ]; then
            echo "OK Phase 4 : Agrégation Machine $ip_machine" >> $log
            break
        fi

        # En cas d'échec, afficher un message d'erreur et réessayer après un délai
        echo "Échec de récupération de reduce_machine${i}.txt depuis la machine $ip_machine, tentative $tentative/$max_tentatives"
        tentative=$((tentative + 1))
        sleep $((tentative * 2))
    done

    # Si la récupération échoue après toutes les tentatives, l'enregistrer dans le log
    if [ $tentative -gt $max_tentatives ]; then
        echo "Impossible de récupérer reduce_machine${i}.txt depuis la machine $ip_machine après $max_tentatives tentatives"
    fi

    # Incrémenter l'index des parties pour passer à la suivante
    i=$((i + 1))
done

# Agrégation des fichiers reduce en un seul fichier local
echo "Agrégation des fichiers reduce..." >> $log
cat "$localFolder"/reduce_machine*.txt > "$fichierResultat"

# Nettoyer les fichiers intermédiaires
rm -f "$localFolder"/reduce_machine*.txt

# Enregistrer le temps d'exécution de la phase d'agrégation
temps_execution $temps_debut_phase "Phase d'agrégation"


# Affichage de la fin de la phase d'agrégation
echo "Fin Phase 4 : Agrégation" >> $log

echo "Résultats disponible dans : $fichierResultat" >> $log

# Début du second MapReduce : tri des résultats

echo "Lancement du deuxième MapReduce pour trier les résultats..." >> $log

# Début du second MapReduce : tri des résultats

echo "Lancement du deuxième MapReduce pour trier les résultats..." >> $log

echo "Début Phase 1 : Mapping (tri)..." >> $log
temps_debut_phase=$(date +%s)

i=1
for ip_machine in "${machinesDisponibles[@]}"; do
    (
        echo "Lancement du mapping (tri) sur la machine $ip_machine" >> $log
        # Copier le fichier d'entrée sur la machine distante
        scp "$sortInputFile" "$login@$ip_machine:$remoteFolder/sort_input_${i}.txt"
        
        # Tentatives de SSH avec délai progressif en cas d'échec
        attempt=1
        max_attempts=5
        success=0
        while [ $attempt -le $max_attempts ]; do
            # Lancer le script Python sur la machine distante
            ssh "$login@$ip_machine" "cd $remoteFolder && python3 $nameOfTheScriptSort mapping_sort $i $remoteFolder/sort_input_${i}.txt $sortOutputFolder/mapping_sort_${i}.txt" && success=1 && break
            
            # Si échec, afficher et réessayer
            echo "Échec de la connexion SSH pour $ip_machine, tentative $attempt/$max_attempts" >> $log
            attempt=$((attempt + 1))
            sleep $((attempt * 2))  # Attendre avant de réessayer (délai progressif)
        done
        
        # Si la tentative est réussie
        if [ $success -eq 1 ]; then
            echo "Mapping (tri) terminé sur la machine $ip_machine" >> $log
        else
            echo "Échec du mapping (tri) sur la machine $ip_machine après $max_attempts tentatives" >> $log
        fi
    ) &  # Exécution en arrière-plan

    i=$((i + 1))  # Incrémenter l'index de la machine
done

# Attendre que tous les processus soient terminés
wait

temps_execution $temps_debut_phase "Mapping (tri)"

# PHASE 2 : SHUFFLING (du tri)
echo "Début Phase 2 : Shuffling (tri)..." >> $log
temps_debut_phase=$(date +%s)

i=1
for ip_machine in "${machinesDisponibles[@]}"; do
    (
        tentative=1
        max_tentatives=5
        reussi=0

        while [ $tentative -le $max_tentatives ]; do
            echo "Tentative $tentative pour le shuffle (tri) sur la machine $ip_machine pour la partie $i" >> $log

            # Lancer la phase de shuffle sur la machine distante
            ssh "$login@$ip_machine" "cd $remoteFolder && python3 $nameOfTheScriptSort shuffle_sort $i $sortOutputFolder $sortOutputFolder $n_machines"

            # Vérifier si le shuffle a réussi
            if [ $? -eq 0 ]; then
                echo "Shuffle (tri) réussi sur la machine $ip_machine pour la partie $i" >> $log
                reussi=1
                break
            else
                echo "Erreur dans le shuffle (tri) sur la machine $ip_machine pour la partie $i à la tentative $tentative/$max_tentatives" >> $log
                sleep $((tentative * 2)) # Attendre avant de réessayer
            fi

            tentative=$((tentative + 1))
        done

        # Vérification finale après toutes les tentatives
        if [ $reussi -eq 0 ]; then
            echo "Échec complet du shuffle (tri) sur la machine $ip_machine pour la partie $i après $max_tentatives tentatives" >> $log
        fi
    ) &
    i=$((i + 1))
done
wait

temps_execution $temps_debut_phase "Shuffling (tri)"

# PHASE 3 : REDUCTION (du tri)
echo "Début Phase 3 : Réduction (tri)..." >> $log
temps_debut_phase=$(date +%s)

i=1
for ip_machine in "${machinesDisponibles[@]}"; do
    (
        tentative=1
        max_tentatives=5
        reussi=0

        while [ $tentative -le $max_tentatives ]; do
            echo "Tentative $tentative de réduction (tri) sur la machine $ip_machine pour la partie $i" >> $log

            # Préparation des fichiers d'entrée pour la réduction
            ssh "$login@$ip_machine" "cd $remoteFolder && cat $sortOutputFolder/shuffle_sort_*_from_machine_${i}.txt > $sortOutputFolder/reduce_sort_input_${i}.txt" &&
            ssh "$login@$ip_machine" "cd $remoteFolder && python3 $nameOfTheScriptSort reduce_sort $i $sortOutputFolder/reduce_sort_input_${i}.txt $sortOutputFolder/reduce_sort_${i}.txt"

            # Vérifier si la réduction a réussi
            if [ $? -eq 0 ]; then
                echo "Réduction (tri) réussie sur la machine $ip_machine pour la partie $i" >> $log
                reussi=1
                break
            else
                echo "Erreur dans la réduction (tri) sur la machine $ip_machine pour la partie $i à la tentative $tentative/$max_tentatives" >> $log
                sleep $((tentative * 2))
            fi

            tentative=$((tentative + 1))
        done

        # Vérification finale après toutes les tentatives
        if [ $reussi -eq 0 ]; then
            echo "Échec complet de la réduction (tri) sur la machine $ip_machine pour la partie $i après $max_tentatives tentatives" >> $log
        fi
    ) &
    i=$((i + 1))
done
wait

temps_execution $temps_debut_phase "Réduction (tri)"

# PHASE FINALE : Récupération et agrégation des résultats triés
echo "Récupération des résultats triés..." >> $log
temps_debut_phase=$(date +%s)

rm -f "$finalSortedFile"

i=1
for ip_machine in "${machinesDisponibles[@]}"; do
    tentative=1
    max_tentatives=5
    reussi=0

    while [ $tentative -le $max_tentatives ]; do
        echo "Tentative $tentative pour récupérer reduce_sort_${i}.txt depuis la machine $ip_machine" >> $log

        # Récupérer le fichier depuis la machine distante
        scp "$login@$ip_machine:$sortOutputFolder/reduce_sort_${i}.txt" "$localFolder/"
        
        if [ $? -eq 0 ]; then
            echo "Récupération réussie pour reduce_sort_${i}.txt depuis la machine $ip_machine" >> $log
            reussi=1
            break
        else
            echo "Erreur de récupération pour reduce_sort_${i}.txt depuis la machine $ip_machine, tentative $tentative/$max_tentatives" >> $log
            sleep $((tentative * 2))
        fi

        tentative=$((tentative + 1))
    done

    # Si récupération réussie, ajouter au fichier final
    if [ $reussi -eq 1 ]; then
        cat "$localFolder/reduce_sort_${i}.txt" >> "$finalSortedFile"
        rm -f "$localFolder/reduce_sort_${i}.txt"
    else
        echo "Fichier reduce_sort_${i}.txt manquant après toutes les tentatives" >> $log
    fi

    i=$((i + 1))
done

echo "Fichier trié final généré : $finalSortedFile" >> $log
temps_execution $temps_debut_phase "Agrégation (tri)"