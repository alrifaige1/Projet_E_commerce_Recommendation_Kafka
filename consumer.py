import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, rand, abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import matplotlib.pyplot as plt
import os

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell"

# Charger la configuration Kafka depuis le fichier client.properties
def load_kafka_config(file_path):
    """
    Charge la configuration Kafka à partir d'un fichier properties.

    @param file_path: Chemin vers le fichier client.properties.
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

# Charger la configuration
kafka_config = load_kafka_config("client.properties")
bootstrapServers = kafka_config["bootstrap.servers"]
kafka_topic = kafka_config["topic"]

# Configuration de la session PySpark
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Définir le schéma pour les messages Kafka
schema = StructType([
    StructField("client_id", IntegerType()),
    StructField("produit_consulter_id", IntegerType()),
])

# Lire le stream Kafka
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", bootstrapServers) \
    .option("kafka.sasl.mechanism", kafka_config["sasl.mechanism"]) \
    .option("kafka.security.protocol", kafka_config["security.protocol"]) \
    .option("kafka.sasl.jaas.config", 
            f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_config["sasl.username"]}" password="{kafka_config["sasl.password"]}";') \
    .option("startingOffsets", "earliest") \
    .option("subscribe", kafka_topic) \
    .load()

def update_bar_chart(data):
    """
    Met à jour et affiche un graphique des consultations.

    @param data: Un dictionnaire avec les identifiants des produits comme clés et le nombre de consultations comme valeurs.
    """
    plt.close('all')  # Ferme toutes les figures ouvertes
    fig, ax = plt.subplots(figsize=(8, 6))
    bar_width = 0.5

    ax.bar(data.keys(), data.values(), width=bar_width)
    ax.set_xlabel('Product ID')
    ax.set_ylabel('Number of Consultations')
    ax.set_title('Real-time Product Consultations')

    ax.set_xlim(left=0, right=20)
    ax.set_ylim(bottom=0, top=max(data.values()) + 1)

    ax.set_xticks(range(1, 21))
    ax.set_xticklabels(range(1, 21))

    plt.show(block=True)

def update_bar_chart_recommendations(data):
    """
    Met à jour et affiche un graphique des recommandations.

    @param data: Un dictionnaire avec les identifiants des produits comme clés et le nombre de recommandations comme valeurs.
    """
    plt.close('all')
    fig, ax = plt.subplots(figsize=(8, 6))
    bar_width = 0.5

    ax.bar(data.keys(), data.values(), width=bar_width, color='red')
    ax.set_xlabel('Product ID')
    ax.set_ylabel('Number of Recommendations')
    ax.set_title('Real-time Product Recommendations')

    ax.set_xlim(left=0, right=20)
    ax.set_ylim(bottom=0, top=max(data.values()) + 1)

    ax.set_xticks(range(1, 21))
    ax.set_xticklabels(range(1, 21))

    plt.show(block=True)

# Comptages des consultations et recommandations
consultation_counts = {}
recommendations_counts = {}

def process_batch_message(df, batch_id):
    """
    Traite un lot de messages Kafka, met à jour les statistiques des consultations et génère des recommandations.

    @param df: DataFrame contenant les messages du lot.
    @param batch_id: Identifiant unique pour le lot traité.
    """
    decoded_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Chargement des données de produits
    produits_df = spark.read.csv("produit.csv", header=True, inferSchema=True).withColumn("Price", col("Price").cast(FloatType()))

    recommendations = decoded_df.join(produits_df, decoded_df.produit_consulter_id == produits_df.ID, "left_outer") \
       .select("client_id", "produit_consulter_id", "Product_Name", "Price", "Category")

    print("\nBatch ID :", batch_id)

    # Mise à jour des consultations
    for row in recommendations.collect():
        produit_consulter_id = row["produit_consulter_id"]
        consultation_counts[produit_consulter_id] = consultation_counts.get(produit_consulter_id, 0) + 1

    update_bar_chart(consultation_counts)

    # Générer des recommandations
    # Parcours de chaque ligne du DataFrame contenant les consultations
    for row in recommendations.collect():  
        # Récupérer les informations de la ligne actuelle
        client_id = row["client_id"]  # Identifiant unique du client
        produit_consulter_id = row["produit_consulter_id"]  # Identifiant du produit consulté
        product_name = row["Product_Name"]  # Nom du produit consulté
        product_price = row["Price"]  # Prix du produit consulté
        product_category = row["Category"]  # Catégorie du produit consulté

        # Vérifier si la catégorie et le prix du produit sont disponibles
        if product_category and product_price is not None:  
            # Filtrer les produits de la même catégorie, mais exclure le produit consulté
            similar_product = produits_df.filter((col("Category") == product_category) & 
                                                 (col("ID") != produit_consulter_id)) \
                                         .withColumn("price_diff", abs(col("Price") - product_price)) \
                                         .orderBy("price_diff") \
                                         .select("ID", "Product_Name", "Price", "Category") \
                                         .limit(1).first()
            """
            .withColumn("price_diff", abs(col("Price") - product_price)) \ # Calculer la différence absolue entre les prix des produits
            .orderBy("price_diff") \ # Trier les produits par différence de prix croissante
            .select("ID", "Product_Name", "Price", "Category") \ #Sélectionner uniquement les colonnes nécessaires pour la recommandation
            .limit(1).first() # Limiter la sélection au produit avec le prix le plus proche                       
            """
            # Si un produit similaire a été trouvé
            if similar_product:
                # Extraire les informations du produit recommandé
                similar_product_id = similar_product["ID"]  # ID du produit recommandé
                similar_product_name = similar_product["Product_Name"]  # Nom du produit recommandé
                similar_product_price = similar_product["Price"]  # Prix du produit recommandé
                similar_product_category = similar_product["Category"]  # Catégorie du produit recommandé

                # Afficher dans le terminal les informations du produit consulté et du produit recommandé
                print(f"Client {client_id} consulted {product_name} "
                    f"(ID: {produit_consulter_id}, Category: {product_category}, Price: {product_price}).")
                print(f"Recommended: {similar_product_name} "
                    f"(ID: {similar_product_id}, Category: {similar_product_category}, Price: {similar_product_price}).")

                # Mettre à jour le compteur de recommandations pour le produit recommandé
                recommendations_counts[similar_product_id] = recommendations_counts.get(similar_product_id, 0) + 1

    # Mettre à jour le graphique des recommandations avec les nouvelles données
    update_bar_chart_recommendations(recommendations_counts)

# Écriture des résultats du streaming
query = streaming_df.writeStream \
    .foreachBatch(process_batch_message) \
    .outputMode("append") \
    .start()

# Attente de fin de la requête
query.awaitTermination()
