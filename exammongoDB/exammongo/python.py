from pymongo import MongoClient
from pprint import pprint

client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username="admin",
    password="pass"
)

# Liste des noms de bases de données disponibles
database_names = client.list_database_names()

# Affichage des noms de bases de données
for db_name in database_names:
    print(db_name)

# Selection de la DB books
db = client.sample

#Accéder à la collection books
books_list = list(db.books.find())

#Récupérer les documents dans books
pprint(books_list)


count = db.books.count_documents({})
print("Nombre total de documents dans la collection 'books':", count)

plus_400pages = db.books.count_documents({"pageCount": {"$gt":400}})

print("nombres de livres avec plus de 400pages :", plus_400pages)

plus_400pages_publish = db.books.count_documents({"pageCount": {"$gt": 400}, "status": "PUBLISH"})

print("nombres de livres de plus de 400 pages publiés", plus_400pages_publish)

# Nombre de livres ayant le mot-clé "Android" dans leur description
count_android_books = db.books.count_documents({
    "$or": [
        {"shortDescription": {"$regex": "Android", "$options": "i"}},
        {"longDescription": {"$regex": "Android", "$options": "i"}}
    ]
})

# Affichage du nombre de livres
print("Nombre de livres ayant le mot-clé 'Android' dans leur description :", count_android_books)

# Utilisation de l'opérateur $group pour regrouper les catégories par index
pipeline = [
    {
        "$group": {
            "_id": None,
            "categorie0": {"$addToSet": {"$arrayElemAt": ["$categories", 0]}},
            "categorie1": {"$addToSet": {"$arrayElemAt": ["$categories", 1]}}
        }
    }
]

# Exécution de la requête avec aggregate()
result = list(db.books.aggregate(pipeline))

# Affichage des résultats
if result:
    categories_0 = result[0]["categorie0"]
    categories_1 = result[0]["categorie1"]
    print("Liste des catégories pour l'index 0 :", categories_0)
    print("Liste des catégories pour l'index 1 :", categories_1)

# Liste des noms de langages à rechercher

langages = ["Python", "Java", "C++", "Scala"]

# Construction de la requête
filtre_regex = {
    "$or": [
        {"longDescription": {"$regex": langage, "$options": "i"}} for langage in langages
    ]
}

# Nombre de livres contenant des noms de langages dans leur description longue
nombre_livres = db.books.count_documents(filtre_regex)

# Affichage du nombre de livres
print("Nombre de livres contenant des noms de langages dans leur description longue :", nombre_livres)

# Pipeline d'agrégation pour obtenir les statistiques par catégorie
pipeline = [
    # Décomposer chaque document en une ligne par catégorie
    {"$unwind": "$categories"},
    # Grouper les documents par catégorie et calculer les statistiques
    {"$group": {
        "_id": "$categories",
        "nombre_livres": {"$sum": 1},
        "nombre_pages_max": {"$max": "$pageCount"},
        "nombre_pages_min": {"$min": "$pageCount"},
        "nombre_pages_moyen": {"$avg": "$pageCount"}
    }}
]

# Exécution de la requête avec aggregate()
resultats = list(db.books.aggregate(pipeline))

# Affichage des résultats
for resultat in resultats:
    print("Catégorie :", resultat["_id"])
    print("Nombre de livres :", resultat["nombre_livres"])
    print("Nombre maximal de pages :", resultat["nombre_pages_max"])
    print("Nombre minimal de pages :", resultat["nombre_pages_min"])
    print("Nombre moyen de pages :", resultat["nombre_pages_moyen"])
    print("-----------------------------------------")


# Pipeline d'agrégation pour extraire les informations de la date de publication et filtrer les livres publiés après 2009
pipeline = [
    # Filtrer les livres publiés après 2009
    {"$match": {"$expr": {"$gt": [{"$year": "$publishedDate"}, 2009]}}},
    # Créer de nouvelles variables pour l'année, le mois et le jour
    {"$project": {
        "year": {"$year": "$publishedDate"},
        "month": {"$month": "$publishedDate"},
        "day": {"$dayOfMonth": "$publishedDate"},
        "title": 1,  # Conserver le titre du livre
        "_id": 0  # Ne pas inclure l'identifiant dans les résultats
    }},
    # Limiter les résultats aux 20 premiers
    {"$limit": 20}
]

# Exécution de la requête avec aggregate()
resultats = list(db.books.aggregate(pipeline))

# Affichage des résultats
for resultat in resultats:
    print("Titre :", resultat["title"])
    print("Année de publication :", resultat["year"])
    print("Mois de publication :", resultat["month"])
    print("Jour de publication :", resultat["day"])
    print("-----------------------------------------")


# Pipeline d'agrégation pour créer de nouveaux attributs d'auteur et limiter les résultats aux 20 premiers dans l'ordre chronologique
pipeline = [
    # Créer de nouveaux attributs d'auteur (author1, author2, ..., author_n)
    {"$project": {
        "authors": 1,  # Conserver la liste des auteurs
        "publishedDate": 1,  # Conserver la date de publication
        "author_count": {"$size": "$authors"}  # Compter le nombre d'auteurs
    }},
    # Limiter les résultats aux 20 premiers dans l'ordre chronologique
    {"$sort": {"publishedDate": 1}},  # Tri dans l'ordre chronologique
    {"$limit": 20},
    # Ajouter de nouveaux attributs d'auteur
    {"$project": {
        "publishedDate": 1,  # Conserver la date de publication
        "author_count": 1,  # Conserver le nombre d'auteurs
        "authors": 1,  # Conserver la liste des auteurs
        "author1": {"$arrayElemAt": ["$authors", 0]},  # Premier auteur
        "author2": {"$arrayElemAt": ["$authors", 1]},  # Deuxième auteur
        # Ajouter d'autres auteurs si le nombre d'auteurs est supérieur à 2
        "author3": {"$arrayElemAt": ["$authors", 2]},
        "author4": {"$arrayElemAt": ["$authors", 3]},
        # Ajouter d'autres auteurs si le nombre d'auteurs est supérieur à 3, et ainsi de suite...
    }}
]

# Exécution de la requête avec aggregate()
resultats = list(db.books.aggregate(pipeline))

# Affichage des résultats
for resultat in resultats:
    if "publishedDate" in resultat:
        print("Date de publication :", resultat["publishedDate"])
    else:
        print("Date de publication : N/A (Non disponible)")
    print("Nombre d'auteurs :", resultat["author_count"])
    print("Auteurs :", resultat["authors"])
    print("Premier auteur :", resultat.get("author1", "N/A"))
    print("Deuxième auteur :", resultat.get("author2", "N/A"))
    print("-----------------------------------------")


# Pipeline d'agrégation pour créer une colonne contenant le nom du premier auteur et agréger selon cette colonne
pipeline = [
    # Créer une colonne contenant le nom du premier auteur
    {"$project": {
        "first_author": {"$arrayElemAt": ["$authors", 0]},  # Premier auteur
        "title": 1  # Conserver le titre du livre
    }},
    # Regrouper les documents par nom du premier auteur et compter le nombre d'articles
    {"$group": {
        "_id": "$first_author",
        "nombre_publications": {"$sum": 1}
    }},
    # Trier les résultats par nombre de publications (ordre décroissant)
    {"$sort": {"nombre_publications": -1}},
    # Limiter les résultats aux 10 premiers
    {"$limit": 10}
]

# Exécution de la requête avec aggregate()
resultats = list(db.books.aggregate(pipeline))

# Affichage des résultats
for resultat in resultats:
    print("Auteur :", resultat["_id"])
    print("Nombre de publications :", resultat["nombre_publications"])
    print("-----------------------------------------")


# Pipeline d'agrégation pour créer une nouvelle colonne avec le nombre d'auteurs et agréger sur cette colonne
pipeline = [
    # Créer une nouvelle colonne avec le nombre d'auteurs
    {"$project": {
        "nombre_auteurs": {"$size": "$authors"}  # Taille de la liste des auteurs
    }},
    # Agréger sur la colonne avec l'accumulateur $count pour obtenir la distribution du nombre d'auteurs
    {"$group": {
        "_id": "$nombre_auteurs",
        "nombre_livres": {"$count": {}}
    }},
    # Trier les résultats par nombre d'auteurs
    {"$sort": {"_id": 1}}
]

# Exécution de la requête avec aggregate()
resultats = list(db.books.aggregate(pipeline))

# Affichage des résultats
for resultat in resultats:
    print("Nombre d'auteurs :", resultat["_id"])
    print("Nombre de livres :", resultat["nombre_livres"])
    print("-----------------------------------------")


