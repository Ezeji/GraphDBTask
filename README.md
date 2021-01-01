# GraphDBTask
The task for this role requires creating an azure cosmos graph database to store airline routes data.
Then perform a count on adjacent city connection routes from the data. 
Also, the task requires creating an azure cosmosDB trigger function for listening to any changes on the database as well as keeping track of the changes through a log system.
To solve the problem, the first thing was to model the database using the concepts of a graph database.
For the airline data which basically is a one-to-one relationship in certain cases and one-to-many relationship required the grouping of route data based on cities to achieve a count for each city.
For instance, route data for City 3 was grouped or labeled as "city3" in a vertex on the graph. This therefore makes the count query to be executed based on various city's route labels thus solving the problem at hand.
For the follow up question, a cosmos trigger function was created in azure portal using the cosmos db trigger function template and the code setup in azure for listening and logging of changes from the task database into another graph within same database.

**Access short video explaining solution to the task here:**
https://drive.google.com/drive/folders/1VPrmrtJ6y4lwPHusRiJ8mQ7IQssgKVj-?usp=sharing