import sys
import os
import re
import hashlib

# Fonction pour obtenir une clé de partition stable
def get_partition_key(word, num_machines):
    return int(hashlib.sha256(word.encode('utf-8')).hexdigest(), 16) % num_machines

# Phase 1 : Fonction de mapping
def map_phase_sort(input_file, output_file):
    """
    Fonction pour mapper les mots et leurs occurrences
    en les reformulant sous forme (occurrence, mot), 
    et effectuer un tri local avant d'écrire les résultats.
    """
    mapped_data = []

    try:
        # Lire le fichier d'entrée
        with open(input_file, 'r') as file:
            for line in file:
                try:
                    # Extraire le mot et le nombre d'occurrences
                    word, count = line.strip().split()
                    count = int(count)
                    # Reformater sous (occurrence, mot)
                    mapped_data.append((count, word))
                except ValueError:
                    print(f"Ligne mal formée ignorée : {line.strip()}")

        # Effectuer un tri local sur les données mappées
        # Trie par occurrence décroissante, puis par mot en ordre alphabétique
        mapped_data.sort(key=lambda x: (-x[0], x[1]))

        # Vérification que le répertoire de sortie existe, sinon le créer
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Écriture des résultats de mapping triés
        with open(output_file, 'w') as file:
            for count, word in mapped_data:
                file.write(f"{count} {word}\n")

        print(f"Fichier de mapping généré et trié : {output_file}")

    except Exception as e:
        print(f"Erreur lors de la phase de mapping : {e}")


# Phase 2 : Fonction de shuffling
def shuffle_phase_sort(machine_id, input_folder, output_folder, num_machines):
    """
    Fonction pour répartir les données de manière équitable entre les machines
    tout en appliquant un tri local avant l'écriture des fichiers de shuffle.
    """
    partitions = {i: [] for i in range(num_machines)}  # Crée un dictionnaire pour les partitions

    try:
        # Parcourir les fichiers de mapping dans le dossier d'entrée
        for filename in os.listdir(input_folder):
            if filename.startswith("mapping_sort_"):
                with open(os.path.join(input_folder, filename), 'r') as file:
                    for line in file:
                        try:
                            count, word = line.strip().split()
                            count = int(count)
                            # Calculer la partition en fonction du hachage stable
                            partition_key = get_partition_key(word, num_machines)
                            partitions[partition_key].append((count, word))
                        except ValueError:
                            print(f"Ligne mal formée ignorée dans {filename}: {line.strip()}")

        # Écriture des fichiers de shuffle pour chaque machine, après avoir trié les partitions
        for target_machine_id in range(num_machines):
            # Trier les partitions localement : par nombre d'occurrences décroissant, puis par mot
            partitions[target_machine_id].sort(key=lambda x: (-x[0], x[1]))

            # Définir le fichier de sortie pour cette partition
            output_file = os.path.join(output_folder, f"shuffle_sort_{target_machine_id + 1}_from_machine_{machine_id}.txt")
            
            # Écrire les données triées dans le fichier de shuffle
            with open(output_file, 'w') as file:
                for count, word in partitions[target_machine_id]:
                    file.write(f"{count} {word}\n")
            print(f"Fichier de shuffle généré : {output_file}")

    except Exception as e:
        print(f"Erreur lors de la phase de shuffling : {e}")

# Phase 3 : Fonction de réduction
def reduce_phase_sort(input_file, output_file):
    """
    Fonction pour trier les données (occurrence, mot) par ordre décroissant
    des occurrences, et alphabétiquement en cas d'égalité, tout en procédant à un tri progressif.
    """
    sorted_data = []  # Liste pour stocker les données triées localement

    try:
        if not os.path.exists(input_file):
            print(f"Le fichier {input_file} n'existe pas.")
            return

        with open(input_file, 'r') as file:
            for line in file:
                try:
                    count, word = line.strip().split()
                    count = int(count)
                    sorted_data.append((count, word))  # Ajouter les données à la liste
                except ValueError:
                    print(f"Ligne mal formée ignorée : {line.strip()}")

        # Tri local après lecture de chaque fichier : par occurrence décroissante, puis par mot
        sorted_data.sort(key=lambda x: (-x[0], x[1]))  # Tri local des données

        # Vérification que le répertoire de sortie existe, sinon le créer
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Écriture des résultats triés dans le fichier de sortie
        with open(output_file, 'w') as file:
            for count, word in sorted_data:
                file.write(f"{count} {word}\n")  # Assurez-vous que chaque ligne est écrite correctement

        print(f"Fichier de réduction généré : {output_file}")

    except Exception as e:
        print(f"Erreur lors de la phase de réduction : {e}")

# Point d'entrée principal du script
if __name__ == "__main__":
    # Vérifier que les arguments sont suffisants
    if len(sys.argv) < 5:
        print("Usage : python3 script_sort.py <phase> <machine_id> <input_path> <output_path> [num_machines]")
        sys.exit(1)

    # Lire les arguments
    phase = sys.argv[1]
    machine_id = int(sys.argv[2])
    input_path = sys.argv[3]
    output_path = sys.argv[4]

    # Vérification si le chemin d'entrée existe
    if not os.path.exists(input_path):
        print(f"Le chemin d'entrée {input_path} n'existe pas.")
        sys.exit(1)
    
    # Vérification si le chemin de sortie est valide (répertoire)
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        print(f"Le répertoire de sortie {output_dir} n'existe pas, création du répertoire.")
        os.makedirs(output_dir)

    # Phase 1 : Mapping (tri)
    if phase == "mapping_sort":
        print(f"Début Phase 1 : Mapping pour le tri sur la machine {machine_id}")
        map_phase_sort(input_path, output_path)
        print(f"Phase 1 terminée : Mapping pour le tri sur la machine {machine_id}")
    
    # Phase 2 : Shuffle (tri)
    elif phase == "shuffle_sort":
        if len(sys.argv) != 6:
            print("Erreur : Nombre de machines (num_machines) requis pour la phase de shuffle.")
            sys.exit(1)
        
        num_machines = int(sys.argv[5])
        if num_machines <= 0:
            print("Le nombre de machines doit être un entier positif.")
            sys.exit(1)
        
        print(f"Début Phase 2 : Shuffling pour le tri sur la machine {machine_id}")
        shuffle_phase_sort(machine_id, input_path, output_path, num_machines)
        print(f"Phase 2 terminée : Shuffling pour le tri sur la machine {machine_id}")
    
    # Phase 3 : Réduction (tri)
    elif phase == "reduce_sort":
        print(f"Début Phase 3 : Réduction pour le tri sur la machine {machine_id}")
        reduce_phase_sort(input_path, output_path)
        print(f"Phase 3 terminée : Réduction pour le tri sur la machine {machine_id}")
    
    # Si la phase n'est pas reconnue
    else:
        print(f"Phase non reconnue : {phase}")
        sys.exit(1)
