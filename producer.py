import configparser
from kafka import KafkaProducer
import json
import random
import time

def load_kafka_config(file_path):
    """
    Charge la configuration Kafka à partir d'un fichier properties.

    @param file_path: Chemin vers le fichier client.properties contenant les paramètres Kafka.
    @return: Un dictionnaire contenant les paramètres de configuration pour Kafka.
    """
    config = configparser.ConfigParser()
    config.read(file_path)
    return {
        "bootstrap.servers": config.get("DEFAULT", "bootstrap.servers"),
        "sasl.mechanism": config.get("DEFAULT", "sasl.mechanism"),
        "security.protocol": config.get("DEFAULT", "security.protocol"),
        "sasl.username": config.get("DEFAULT", "sasl.username"),
        "sasl.password": config.get("DEFAULT", "sasl.password"),
        "topic": config.get("DEFAULT", "topic"),
    }

def main():
    """
    Fonction principale pour produire un nombre défini d'événements Kafka.

    Cette fonction permet à l'utilisateur de spécifier combien de messages doivent être produits. Chaque message
    simule une consultation d'un produit par un client et est envoyé à un topic Kafka configuré.

    @return: None
    """
    # Charger la configuration Kafka
    kafka_config = load_kafka_config("client.properties")

    # Initialiser le producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap.servers"],
        security_protocol=kafka_config["security.protocol"],
        sasl_mechanism=kafka_config["sasl.mechanism"],
        sasl_plain_username=kafka_config["sasl.username"],
        sasl_plain_password=kafka_config["sasl.password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # Sérialisation des messages en JSON
    )

    # Liste d'ID de produits disponibles (de 1 à 19 inclus)
    liste_ids_produits = list(range(1, 20))

    # Demander à l'utilisateur le nombre de messages à produire
    try:
        num_messages = int(input("Entrez le nombre de messages à produire: "))
        if num_messages <= 0:
            raise ValueError("Le nombre de messages doit être supérieur à zéro.")
    except ValueError as e:
        print(f"Erreur : {e}. Réessayez avec un nombre valide.")
        return

    try:
        for _ in range(num_messages):
            """
            Boucle pour simuler des consultations de produits.
            """
            # Générer un message simulant la consultation d'un produit
            produit_consulte = {
                "client_id": random.randint(1, 1000),  # Génération d'un ID aléatoire pour le client
                "produit_consulter_id": random.choice(liste_ids_produits),  # Sélection d'un produit aléatoire
            }

            # Envoyer le message au topic Kafka
            producer.send(kafka_config["topic"], value=produit_consulte)
            print(f"Produit consulté par le client {produit_consulte['client_id']} : Produit {produit_consulte['produit_consulter_id']}")

            # Pause aléatoire entre les envois pour simuler un délai naturel
            time.sleep(random.uniform(1, 5))

        print(f"Production terminée : {num_messages} messages produits.")
    except KeyboardInterrupt:
        """
        Gestion de l'interruption par l'utilisateur (Ctrl+C).
        """
        print("Interruption du producteur Kafka.")
    finally:
        """
        Fermeture propre du producteur Kafka en cas d'arrêt ou d'erreur.
        """
        producer.close()

# Point d'entrée du script
if __name__ == "__main__":
    main()
