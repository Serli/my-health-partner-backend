my-health-partner-backend
=========================

Description :
-------------
*************

This application permits to store the data sent by the data set mobile application and it can recognize the activity 
from the data sent by the user mobile application.

### Storage :

It receives the accelerometer and profile data and insert them into a Cassandra database.
Then it computes the features associated and store them into the database.

### Recognize :

It receives the accelerometer data, computes the features associated and uses the machine learning algorithm to 
recognize the activity performed by the user.

Developer Guide :
-----------------
*****************

### Deployment :

The deployment of the application is automatised with docker and docker-compose.
Clone the repo and run ``docker-compose up -d``

### Change port :
If you want to change the access port of the application, you have to change the port exposed by the spring container in
docker-compose. In docker-compose.yml, change at the line 11 the default port (80) by the port wanted.