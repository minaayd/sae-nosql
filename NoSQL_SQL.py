#!/usr/bin/env python
# coding: utf-8

# # SAE - Migration de données vers NoSQL

# Mina AYDOGMUS - Magui AZMIRLY
# BUT VCOD 34

# In[2]:


# Importation des modules utilisés
import sqlite3
import pandas

# Création de la connexion
conn=sqlite3.connect("C:/Users/Mina/Downloads/ClassicModel.sqlite")


# # 1. Lister les clients n’ayant jamais effecuté une commande ;

# In[7]:


pandas.read_sql_query("""
SELECT c.customerNumber, c.customerName
FROM Customers c 
left join Orders o on c.customerNumber=o.customerNumber
WHERE orderNumber is null
;""", conn)


# # 2. Pour chaque employé, le nombre de clients, le nombre de commandes et le montant total de celles-ci ;

# In[8]:


pandas.read_sql_query("""
select e.employeeNumber, e.firstName, e.lastName,count(distinct c.customerNumber) as NbCustomers, 
count(distinct o.orderNumber) as NbOrders, sum(p.amount) as TtAmount from Employees e
LEFT JOIN Customers c on e.employeeNumber = c.salesRepEmployeeNumber
LEFT JOIN Orders o on c.customerNumber = o.customerNumber
LEFT JOIN Payments p on o.customerNumber = p.customerNumber
GROUP BY 1,2, 3
;""", conn)


# # 3. Idem pour chaque bureau (nombre de clients, nombre de commandes et montant total), avec en plus le nombre de clients d’un pays différent, s’il y en a ;
# 

# In[5]:


pandas.read_sql_query("""
SELECT O.officeCode, O.city, O.country,count(distinct c.customerNumber) as NbCustomers, count(distinct ord.orderNumber) as NbOrder, 
sum(p.amount) as TtAmount,  COUNT(DISTINCT CASE WHEN o.country != c.country THEN c.customerNumber END) as customersFromDifferentCountry
FROM Offices o LEFT JOIN Employees e on o.officeCode=e.officeCode
LEFT JOIN Customers c on c.salesRepEmployeeNumber=e.employeeNumber
LEFT JOIN Orders ord on ord.customerNumber=c.customerNumber
LEFT JOIN Payments p on ord.customerNumber = p.customerNumber
GROUP BY o.officeCode
;""", conn)


# # 4. Pour chaque produit, donner le nombre de commandes, la quantité totale commandée, et le nombre de clients différents ;

# In[52]:


pandas.read_sql_query("""
SELECT pr.productCode, pr.productName, count(distinct ord.orderNumber) as NbOrder, 
sum(od.quantityOrdered) as TtQuantity, count(distinct ord.customerNumber) as NbDistinctCustomers
FROM Products pr LEFT JOIN OrderDetails od on pr.productCode=od.productCode
LEFT JOIN Orders Ord on od.orderNumber=ord.orderNumber
GROUP BY 1, 2
;""", conn)


# # 5. Donner le nombre de commande pour chaque pays du client, ainsi que le montant total des commandes et le montant total payé : on veut conserver les clients n’ayant jamais commandé dans le résultat final ;
# 

# In[6]:


pandas.read_sql_query("""
SELECT c.country, 
       COUNT(DISTINCT o.orderNumber) AS NbOrders,
       COALESCE(SUM(od.quantityOrdered * od.priceEach), 0) AS TotalOrderAmount,
       COALESCE(SUM(p.amount), 0) AS TotalPaidAmount
FROM Customers c
LEFT JOIN Orders o ON c.customerNumber = o.customerNumber
LEFT JOIN OrderDetails od ON o.orderNumber = od.orderNumber
LEFT JOIN Payments p ON c.customerNumber = p.customerNumber
GROUP BY c.country
ORDER BY c.country;""", conn)


# # 6. On veut la table de contigence du nombre de commande entre la ligne de produits et le pays du client ;

# In[40]:


pandas.read_sql_query("""
SELECT p.productLine, c.country, COUNT(DISTINCT o.orderNumber) AS NbOrders
FROM Products p
LEFT JOIN OrderDetails od ON p.productCode = od.productCode
LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
GROUP BY p.productLine, c.country
ORDER BY p.productLine, c.country;""", conn)


# # 7. On veut la même table croisant la ligne de produits et le pays du client, mais avec le montant total payé dans chaque cellule ;

# In[8]:


pandas.read_sql_query("""
SELECT p.productLine,c.country,COALESCE(SUM(pay.amount), 0) AS TotalAmount
FROM Products p
LEFT JOIN OrderDetails od ON p.productCode = od.productCode
LEFT JOIN Orders o ON od.orderNumber = o.orderNumber
LEFT JOIN Customers c ON o.customerNumber = c.customerNumber
LEFT JOIN Payments pay ON c.customerNumber = pay.customerNumber
GROUP BY p.productLine, c.country
ORDER BY p.productLine, c.country;""", conn)


# # 8. Donner les 10 produits pour lesquels la marge moyenne est la plus importante;

# In[9]:


pandas.read_sql_query("""
SELECT p.productCode, p.productName, 
       AVG(od.priceEach - p.buyPrice) AS marge_moy
FROM Products p
JOIN OrderDetails od ON p.productCode = od.productCode
GROUP BY p.productCode, p.productName
ORDER BY marge_moy DESC
LIMIT 10;
""", conn)


# # 9. Lister les produits (avec le nom et le code du client) qui ont été vendus à perte : Si un produit a été dans cette situation plusieurs fois, il doit apparaître plusieurs fois, Une vente à perte arrive quand le prix de vente est inférieur au prix d’achat ;

# In[4]:


pandas.read_sql_query("""
SELECT P.productCode, P.productName, C.customerName, C.customerNumber,  OD.priceEach, P.buyPrice 
FROM Products as P left join OrderDetails as OD on P.productCode=OD.productCode left join Orders as O on OD.orderNumber=O.orderNumber
left join Customers as C on C.customerNumber=O.customerNumber
WHERE P.buyPrice > OD.priceEach
""", conn)


# # 10. (bonus) Lister les clients pour lesquels le montant total payé est supérieur aux montants totals des achats ;

# In[19]:


pandas.read_sql_query("""
SELECT c.customerNumber, c.customerName, 
       SUM(p.amount) AS mt_tot_paye,
       SUM(od.priceEach * od.quantityOrdered) AS mt_tot_achats
FROM Customers c
JOIN Payments p ON c.customerNumber = p.customerNumber
JOIN Orders o ON c.customerNumber = o.customerNumber
JOIN OrderDetails od ON o.orderNumber = od.orderNumber
GROUP BY c.customerNumber, c.customerName
HAVING mt_tot_paye > mt_tot_achats
ORDER BY c.customerNumber;
""", conn)


# In[ ]:


#Fermeture de la connexion
conn.close()


# In[ ]:




