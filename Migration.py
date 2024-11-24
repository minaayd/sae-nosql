#!/usr/bin/env python
# coding: utf-8

# # Migration des données de SQL à NoSQL

# In[11]:


# Import des bibliothèques nécessaires
import sqlite3  # Pour la connexion à SQLite
import pandas as pd  # Pour la manipulation des données
import pandas
import pymongo  # Pour la connexion à MongoDB
get_ipython().system('pip install -r requirements.txt --quiet  # Installation des dépendances')

# Connexion à MongoDB
client = pymongo.MongoClient('mongodb+srv://user_mongo:F6i16fbYdv8TB4DA@cluster0.6p2ao.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db = client.nosql
# Sélection de la base de données

# Création de la connexion à SQLite
conn = sqlite3.connect("ClassicModel.sqlite")

# Extraction des données depuis SQLite
# Lecture de chaque table et stockage dans des DataFrames pandas
products = pandas.read_sql_query(
    "SELECT * FROM Products;", 
    conn
)

orders = pandas.read_sql_query(
    "SELECT * FROM Orders;", 
    conn
)

customers = pandas.read_sql_query(
    "SELECT * FROM Customers;", 
    conn
)

employees = pandas.read_sql_query(
    "SELECT * FROM Employees;", 
    conn
)

# Jointure naturelle entre Payments et Orders
payments = pandas.read_sql_query(
    "SELECT * FROM Payments NATURAL JOIN Orders;", 
    conn
)

# Jointure naturelle entre Offices et Employees
offices = pandas.read_sql_query(
    "SELECT * FROM Offices NATURAL JOIN Employees;", 
    conn
)

# Jointure naturelle entre OrderDetails et Orders
od = pandas.read_sql_query(
    "SELECT * FROM OrderDetails NATURAL JOIN Orders;", 
    conn
)

# Transformation des données

# Pour chaque commande, ajout des détails de commande correspondants
# Suppression des colonnes redondantes et conversion en dictionnaire
orders['orderDetails'] = [
    od[od['orderNumber'] == on].drop(columns=['orderNumber', 'requiredDate', 'shippedDate', 'status', 'customerNumber', 'orderDate', 'comments']).to_dict(orient='records')
    for on in orders['orderNumber']
]

# Pour chaque commande, ajout des paiements correspondants
# Suppression des colonnes redondantes et conversion en dictionnaire
orders['payments'] = [
    payments[payments['orderNumber'] == on].drop(columns=['orderNumber', 'requiredDate', 'shippedDate', 'status', 'customerNumber', 'orderDate', 'comments']).to_dict(orient='records')
    for on in orders['orderNumber']
]

# Pour chaque employé, ajout des informations de son bureau
# Suppression des colonnes redondantes et conversion en dictionnaire
employees['office'] = [
    offices[offices['employeeNumber'] == en].drop(columns=['employeeNumber', 'lastName', 'firstName', 'extension', 'email', 'reportsTo', 'jobTitle']).to_dict(orient='records')[0]
    if en in offices['employeeNumber'].values else None
    for en in employees['employeeNumber']
]

# Chargement des données dans MongoDB
# Conversion des DataFrames en dictionnaires et insertion dans les collections correspondantes
db.products.insert_many(products.to_dict(orient = "records"))
db.orders.insert_many(orders.to_dict(orient = "records"))
db.employees.insert_many(employees.to_dict(orient = "records"))
db.customers.insert_many(customers.to_dict(orient = "records"))


# ## Question 1: Lister les clients n’ayant jamais effecuté une commande

# In[280]:


q1 = db.customers.aggregate([
  {
    "$lookup": {
      "from": "orders",
      "localField": "customerNumber",
      "foreignField": "customerNumber",
      "as": "customerOrders"
    }
  },
  {
    "$match": {
      "customerOrders": { "$size": 0 }
    }
  },
  {
    "$group": {
      "_id": "$customerNumber",
      "customerName": { "$first": "$customerName" }
    }
  },
  {
    "$project": {
      "_id": 0,
      "customerNumber": "$_id",
      "customerName": 1
    }
  },
  {
    "$sort": { "customerNumber": 1 }  # Ajout de cette étape
  }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q1))

# Affichage du tableau
print(df.to_string())


# ## Question 2: Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci 

# In[23]:


q2 = db.employees.aggregate([
    {
        "$lookup": {
            "from": "customers",
            "localField": "employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "clients"
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "clients.customerNumber",
            "foreignField": "customerNumber",
            "as": "commandes"
        }
    },
    {
        "$project": {
            "_id": 0,
            "firstName": 1,
            "employeeNumber": 1,
            "lastName": 1,
            "numberOfCustomers": { "$size": "$clients" },
            "numberOfOrders": { "$size": "$commandes" },
            "totalOrderAmount": {
                "$sum": {
                    "$map": {
                        "input": "$commandes",
                        "as": "commande",
                        "in": {
                            "$sum": "$$commande.payments.amount"
                        }
                    }
                }
            }
        }
    },
    {
        "$sort": { "employeeNumber": 1 }
    }
])
# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q2))

# Affichage du tableau
print(df.to_string(index=False))


# ## Question 3: Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a 

# In[35]:


q3 = db.employees.aggregate([
    {
        "$lookup": {
            "from": "customers",
            "localField": "employeeNumber",
            "foreignField": "salesRepEmployeeNumber",
            "as": "clients"
        }
    },
    {
        "$lookup": {
            "from": "orders",
            "localField": "clients.customerNumber",
            "foreignField": "customerNumber",
            "as": "commandes"
        }
    },
    {
        "$group": {
            "_id": "$office.officeCode",
            "city": { "$first": "$office.city" },
            "officeCountry": {"$first": "$office.country"}, 
            "numberOfCustomers": { "$sum": { "$size": "$clients" } },
            "numberOfOrders": { "$sum": { "$size": "$commandes" } },
            "totalOrderAmount": {
                "$sum": {
                    "$reduce": {
                        "input": "$commandes",
                        "initialValue": 0,
                        "in": {
                            "$add": [
                                "$$value",
                                { "$sum": "$$this.payments.amount" }
                            ]
                        }
                    }
                }
            },
            "customersFromDifferentCountry": {
                "$sum": {
                    "$size": {
                        "$filter": {
                            "input": "$clients",
                            "as": "client",
                            "cond": { "$ne": ["$$client.country", "$office.country"] }
                        }
                    }
                }
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "officeCode": "$_id",
            "city": 1,
            "officeCountry": 1,
            "numberOfCustomers": 1,
            "numberOfOrders": 1,
            "totalOrderAmount": 1,
            "customersFromDifferentCountry": 1
        }
    },
    {
        "$sort": { "officeCode": 1 }
    }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q3))

# Affichage du tableau
print(df.to_string(index=False))


# ## Question 4: Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents

# In[281]:


q4 = db.orders.aggregate([
    {
        "$unwind": "$orderDetails"
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "orderDetails.productCode",
            "foreignField": "productCode",
            "as": "product"
        }
    },
    {
        "$unwind": "$product"
    },
    {
        "$group": {
            "_id": {
                "productCode": "$product.productCode",
                "productName": "$product.productName"
            },
            "numberOfOrders": { "$addToSet": "$orderNumber" },
            "totalQuantityOrdered": { "$sum": "$orderDetails.quantityOrdered" },
            "distinctCustomers": { "$addToSet": "$customerNumber" }
        }
    },
    {
"$project": {
            "_id": 0,
            "productCode": "$_id.productCode",
            "productName": "$_id.productName",
            "numberOfOrders": { "$size": "$numberOfOrders" },
            "totalQuantityOrdered": 1,
            "numberOfDistinctCustomers": { "$size": "$distinctCustomers" }
        }
    },
    {
        "$sort": { "productCode": 1 }
    }
])

# Conversion en DataFrame pandas
df = pd.DataFrame(list(q4))
print(df.to_string(index=False))


# ## Question 5: Donner le nombre de commande pour chaque pays, ainsi que le montant total des commandes et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final

# In[282]:


q5 =db.customers.aggregate([
  {
    "$lookup": {
      "from": "orders",
      "localField": "customerNumber",
      "foreignField": "customerNumber",
      "as": "customerOrders"
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders.orderDetails",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders.payments",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$group": {
      "_id": "$country",
"numberOfOrders": { "$addToSet": "$customerOrders.orderNumber" },
      "totalOrderAmount": {
        "$sum": {
          "$multiply": [
            { "$ifNull": ["$customerOrders.orderDetails.quantityOrdered", 0] },
            { "$ifNull": ["$customerOrders.orderDetails.priceEach", 0] }
          ]
        }
      },
      "totalPaidAmount": {
        "$sum": { "$ifNull": ["$customerOrders.payments.amount", 0] }
      }
    }
  },
  {
    "$project": {
      "_id": 0,
      "country": "$_id",
      "numberOfOrders": { "$size": "$numberOfOrders" },
      "totalOrderAmount": 1,
      "totalPaidAmount": 1
    }
  },
  {
    "$sort": { "country": 1 }
  }
])
df = pd.DataFrame(list(q5))
print(df.to_string(index=False))


# ## Question 6: On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client

# In[283]:


q6 = db.orders.aggregate([
    {
        "$unwind": "$orderDetails"
    },
    {
        "$lookup": {
            "from": "customers",
            "localField": "customerNumber",
            "foreignField": "customerNumber",
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "orderDetails.productCode",
            "foreignField": "productCode",
            "as": "product"
        }
    },
    {
        "$unwind": "$product"
    },
    {
        "$match": {
            "product.productLine": {"$in": ["Classic Cars", "Vintage Cars"]}
        }
    },
    {
        "$group": {
            "_id": {
                "productLine": "$product.productLine",
                "country": "$customer.country",
                "orderNumber": "$orderNumber"
            }
        }
    },
    {
        "$group": {
            "_id": {
                "productLine": "$_id.productLine",
                "country": "$_id.country"
            },
            "numberOfOrders": { "$sum": 1 }
        }
    },
    {
        "$group": {
            "_id": "$_id.productLine",
            "countries": { "$push": { "country": "$_id.country", "numberOfOrders": "$numberOfOrders" } }
        }
    },
    {
        "$project": {
            "productLine": "$_id",
            "countries": {
                "$concatArrays": [
                    [{ "country": "None", "numberOfOrders": 0 }],
                    "$countries"
                ]
            }
        }
    },
    {
        "$unwind": "$countries"
    },
    {
        "$project": {
            "_id": 0,
            "productLine": 1,
            "country": "$countries.country",
            "numberOfOrders": "$countries.numberOfOrders"
        }
    },
    {
        "$sort": { "productLine": 1, "country": 1 }
    }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q6))

# Affichage du tableau
print(df.to_string(index=False))


# ## Question 7: On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule

# In[284]:


q7 = db.orders.aggregate([
    {
        "$unwind": "$orderDetails"
    },
    {
        "$lookup": {
            "from": "customers",
            "localField": "customerNumber",
            "foreignField": "customerNumber",
            "as": "customer"
        }
    },
    {
        "$unwind": "$customer"
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "orderDetails.productCode",
            "foreignField": "productCode",
            "as": "product"
        }
    },
    {
        "$unwind": "$product"
    },
    {
        "$group": {
            "_id": {
                "productLine": "$product.productLine",
                "country": "$customer.country"
            },
            "totalPaidAmount": {
                "$sum": { "$sum": "$payments.amount" }
            }
        }
    },
    {
        "$group": {
            "_id": "$_id.productLine",
            "countries": {
                "$push": {
                    "country": "$_id.country",
                    "totalPaidAmount": "$totalPaidAmount"
                }
            },
            "totalPaidAmount": { "$sum": "$totalPaidAmount" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "productLine": "$_id",
            "countries": {
                "$concatArrays": [
                    [{ "country": "None", "totalPaidAmount": 0 }],
                    "$countries"
                ]
            }
        }
    },
    {
        "$unwind": "$countries"
    },
    {
        "$project": {
            "productLine": 1,
            "country": "$countries.country",
            "totalPaidAmount": "$countries.totalPaidAmount"
        }
    },
    {
        "$sort": { "productLine": 1, "country": 1 }
    }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q7))

# Affichage du tableau sans arrondir les montants
pd.set_option('display.float_format', '{:.2f}'.format)
print(df.to_string(index=False))


# ## Question 8: Donner les 10 produits pour lesquels la marge moyenne est la plus importante (cf buyPrice et priceEach) 

# In[285]:


q8=db.orders.aggregate([
  { "$unwind": "$orderDetails" },
  {
    "$lookup": {
      "from": "products",
      "localField": "orderDetails.productCode",
      "foreignField": "productCode",
      "as": "product"
    }
  },
  { "$unwind": "$product" },
  {
    "$group": {
      "_id": "$product.productCode",
      "productName": { "$first": "$product.productName" },
      "averageMargin": {
        "$avg": {
          "$subtract": [
            "$orderDetails.priceEach",
            "$product.buyPrice"
          ]
        }
      }
    }
  },
  { "$sort": { "averageMargin": -1 } },
  { "$limit": 10 },
  {
    "$project": {
      "_id": 0,
      "productCode": "$_id",
      "productName": 1,
      "averageMargin": 1
    }
  }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q8))

# Affichage du tableau sans arrondir les montants
pd.set_option('display.float_format', '{:.6f}'.format)
print(df.to_string(index=False))


# ## Question 9: Lister les produits (avec le nom et le code du client) qui ont été vendus à perte: 
# ### - Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois,
# ### - Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;

# In[286]:


q9=db.orders.aggregate([
  { "$unwind": "$orderDetails" },
  {
    "$lookup": {
      "from": "products",
      "localField": "orderDetails.productCode",
      "foreignField": "productCode",
      "as": "product"
    }
  },
  { "$unwind": "$product" },
  {
    "$match": {
      "$expr": {
        "$lt": ["$orderDetails.priceEach", "$product.buyPrice"]
      }
    }
  },
  {
    "$lookup": {
      "from": "customers",
      "localField": "customerNumber",
      "foreignField": "customerNumber",
      "as": "customer"
    }
  },
  { "$unwind": "$customer" },
  {
    "$project": {
      "_id": 0,
      "productCode": "$orderDetails.productCode",
      "productName": "$product.productName",
      "customerName": "$customer.customerName",
      "customerNumber": "$customer.customerNumber",
      "priceEach": "$orderDetails.priceEach",
      "buyPrice": "$product.buyPrice"
    }
  }
])

# Conversion du résultat en DataFrame pandas
df = pd.DataFrame(list(q9))

# Affichage du tableau sans arrondir les montants
pd.set_option('display.float_format', '{:.2f}'.format)
print(df.to_string(index=False))


# # Question 10: (bonus) Lister les clients pour lesquels le montant total payé est supérieur aux montants totals des achats ;

# In[278]:


q10 = db.customers.aggregate([
  {
    "$lookup": {
      "from": "orders",
      "localField": "customerNumber",
      "foreignField": "customerNumber",
      "as": "customerOrders"
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders.orderDetails",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$unwind": {
      "path": "$customerOrders.payments",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$group": {
      "_id": "$customerNumber",
      "customerName": { "$first": "$customerName" },
      "totalPaid": {
        "$sum": { "$ifNull": ["$customerOrders.payments.amount", 0] }
      },
      "totalPurchased": {
        "$sum": {
          "$multiply": [
            { "$ifNull": ["$customerOrders.orderDetails.quantityOrdered", 0] },
            { "$ifNull": ["$customerOrders.orderDetails.priceEach", 0] }
          ]
        }
      }
    }
  },
  {
    "$match": {
      "$expr": { "$gt": ["$totalPaid", "$totalPurchased"] }
    }
  },
  {
    "$project": {
      "_id": 0,
      "customerNumber": "$_id",
      "customerName": 1,
      "totalPaid": 1,
      "totalPurchased": 1
    }
  },
  {
    "$sort": { "customerNumber": 1 }
  }
])

df = pd.DataFrame(list(q10))
print(df.to_string(index=False))


# In[279]:


#Fermeture de la connexion
conn.close()

