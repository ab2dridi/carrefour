Language utilisé:
------------
* scala


Livrable:
----------
* Application packagée prête à être utilisée.
* Code Source.


Exécution du script:
--------
* Pour exécuter le script on a besoin d'un repertoire de données, dans ce repertoire on a besoin :
 
- des fichiers transactions des 7 derniers jours

- des fichiers référentiels pour chaque magasin pour les 7 derniers jours

* Il faut respecter ces règles de nommage:
 
  - les transactions : `transactions_YYYYMMDD.data`
  
  - les référentiels : `reference_prod_ID-MAGASIN_YYYYMMDD.data` où ID_MAGASIN est un UUID.

 
Pour Exécuter le script on lance la commande suivante :  `java -jar repertoire_du_projet/target/CarrefourChallenge-assembly-0.1.jar run -d repertoire/des/donnees`



output:
--------
* à la fin de l'exécution du code on aura comme résultat:
- un répertoire outputData
- Dans le répertoire outputData on généré 7 dossiers KPI{n}

- Nous trouverons cette liste d'indicateurs: 
	
1. KPI1/`top_100_ventes_<MAGASIN_ID>_YYYYMMDD.data` 
2. KPI2/`top_100_ventes_GLOBAL_YYYYMMDD.data`
3. KPI3/`top_100_ca_<MAGASIN_ID>_YYYYMMDD.data`
4. KPI4/`top_100_ca_GLOBAL_YYYYMMDD.data`
5. KPI5/`top_100_ventes_<MAGASIN_ID>_YYYYMMDD-J7.data` 
6. KPI6/`top_100_ventes_GLOBAL_YYYYMMDD-J7.data`
7. KPI7/`top_100_ca_<MAGASIN_ID>_YYYYMMDD-J7.data`
8. KPI8/`top_100_ca_GLOBAL_YYYYMMDD-J7.data`



Regénération du code source:
--------

* Pour regénérer le code source : 

`cd repertoire_du_projet`
`sbt clean assembly`

  - un jar sera généré dans le repertoire `repertoire_du_projet/target`
  - le nom du jar : `CarrefourChallenge-assembly-0.1.jar`
 
