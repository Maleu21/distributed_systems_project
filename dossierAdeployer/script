import sys
import re
import os
import hashlib

# Phase 1 : Fonction de mapping
def map_phase(input_file, output_file):
    """
    Fonction pour lire un fichier d'entrée, extraire les mots,
    et produire un fichier avec chaque mot suivi de '1' (indiquant une occurrence).
    """
    word_count = []  # Liste pour stocker les résultats du mapping

    try:
        # Lecture du fichier d'entrée en mode binaire pour éviter les problèmes d'encodage
        with open(input_file, 'rb') as file:
            for line in file:
                try:
                    # Décodage de la ligne en UTF-8
                    decoded_line = line.decode('utf-8')
                except UnicodeDecodeError:
                    # Si un caractère non valide est trouvé, on l'ignore
                    decoded_line = line.decode('utf-8', errors='ignore')
                
                # Extraction des mots (en minuscules) à l'aide d'une expression régulière
                words = re.findall(r'\b\w+\b', decoded_line.lower())
                for word in words:
                    word_count.append(f"{word} 1")  # Ajouter le mot avec '1' pour indiquer une occurrence
        
        # Vérification que le répertoire de sortie existe, sinon le créer
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Écriture des résultats dans le fichier de sortie
        with open(output_file, 'w') as file:
            for entry in word_count:
                file.write(entry + '\n')

        print(f"Fichier de mapping généré : {output_file}")

    except Exception as e:
        # Gestion des erreurs éventuelles
        print(f"Erreur lors du mapping : {e}")

# Phase 2 : Fonction de shuffling
def shuffle_phase(machine_id, input_folder, output_folder, num_machines):
    """
    Fonction pour regrouper les mots par hachage et les répartir entre les machines.
    """
    word_map = {}  # Dictionnaire pour regrouper les occurrences des mots

    # Parcours de tous les fichiers de mapping dans le dossier d'entrée
    for filename in os.listdir(input_folder):
        if filename.startswith("mapping_"):  # Vérification que le fichier est un fichier de mapping
            try:
                with open(os.path.join(input_folder, filename), 'r') as file:
                    for line in file:
                        try:
                            # Lecture du mot et de son compte
                            word, count = line.split()
                            word_map[word] = word_map.get(word, 0) + int(count)  # Agrégation des occurrences
                        except ValueError:
                            # Ignorer les lignes mal formées
                            print(f"Ligne mal formée ignorée dans {filename}: {line.strip()}")
            except Exception as e:
                # Gestion des erreurs de lecture
                print(f"Erreur lors de la lecture du fichier {filename}: {e}")

    # Création d'une partition pour chaque machine
    partitions = {i: [] for i in range(num_machines)}

    # Répartition des mots dans les partitions en fonction de leur hachage
    for word, count in word_map.items():
        try:
            # Calcul de la clé de partition en utilisant le hachage MD5
            partition_key = int(hashlib.md5(word.encode()).hexdigest(), 16) % num_machines
            partitions[partition_key].append((word, count))  # Ajouter le mot et son compte à la partition correspondante
        except Exception as e:
            # Gestion des erreurs lors du partitionnement
            print(f"Erreur lors du partitionnement du mot {word}: {e}")

    # Écriture des fichiers de shuffle pour chaque partition
    for target_machine_id in range(num_machines):
        output_file = os.path.join(output_folder, f"shuffle_{target_machine_id + 1}_from_machine_{machine_id}.txt")
        try:
            with open(output_file, 'w') as file:
                for word, count in partitions[target_machine_id]:
                    file.write(f"{word} {count}\n")  # Écrire le mot et son compte dans le fichier de sortie
            print(f"Fichier de shuffle généré : {output_file}")
        except Exception as e:
            # Gestion des erreurs d'écriture
            print(f"Erreur lors de la création du fichier de shuffle {output_file}: {e}")

# Phase 3 : Fonction de réduction
def reduce_phase(input_file, output_file):
    """
    Fonction pour regrouper et additionner les occurrences des mots,
    puis produire un fichier trié par ordre décroissant des occurrences.
    """
    word_count = {}  # Dictionnaire pour stocker les occurrences agrégées des mots

    try:
        # Lecture du fichier d'entrée ligne par ligne
        with open(input_file, 'r') as file:
            for line in file:
                try:
                    # Lecture du mot et de son compte
                    word, count = line.split()
                    word_count[word] = word_count.get(word, 0) + int(count)  # Agrégation des comptes
                except ValueError:
                    # Ignorer les lignes mal formées
                    print(f"Ligne mal formée ignorée dans {input_file}: {line.strip()}")

        # Vérification que le répertoire de sortie existe, sinon le créer
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Tri des mots par ordre décroissant des occurrences, puis alphabétique en cas d'égalité
        sorted_words = sorted(word_count.items(), key=lambda x: (-x[1], x[0]))

        # Écriture des résultats triés dans le fichier de sortie
        with open(output_file, 'w') as file:
            for word, count in sorted_words:
                file.write(f"{word} {count}\n")

        print(f"Fichier de réduction généré : {output_file}")

    except Exception as e:
        # Gestion des erreurs éventuelles
        print(f"Erreur lors de la réduction : {e}")

# Point d'entrée principal du script
if __name__ == "__main__":
    # Vérification du nombre d'arguments passés en ligne de commande
    if len(sys.argv) < 5:
        print("Usage : python3 script.py <phase> <machine_id> <input_path> <output_path> [num_machines]")
        sys.exit(1)
    
    # Lecture des arguments de la ligne de commande
    phase = sys.argv[1]
    machine_id = int(sys.argv[2])
    input_path = sys.argv[3]
    output_path = sys.argv[4]

    # Phase 1 : Mapping
    if phase == "mapping":
        print(f"Début Phase 1: Mapping sur la machine {machine_id}")
        map_phase(input_path, output_path)
        print(f"Phase 1 : Mapping terminé sur la machine {machine_id}")
    
    # Phase 2 : Shuffling
    elif phase == "shuffle":
        if len(sys.argv) != 6:
            print("Erreur : Nombre de machines (num_machines) requis pour la phase shuffle.")
            sys.exit(1)
        num_machines = int(sys.argv[5])
        print(f"Début Phase 2: Shuffling Machine {machine_id} avec {num_machines} machines")
        shuffle_phase(machine_id, input_path, output_path, num_machines)
        print(f"Phase 2 : Shuffling Machine {machine_id}")
    
    # Phase 3 : Reduction
    elif phase == "reduce":
        print(f"Début Phase 3: Reducing Machine {machine_id}")
        reduce_phase(input_path, output_path)
        print(f"Phase 3 : Reducing Machine {machine_id}")
    
    # Phase non reconnue
    else:
        print(f"Phase non reconnue : {phase}")
        sys.exit(1)
