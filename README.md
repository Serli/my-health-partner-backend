my-health-partner-backend
=========================

Description :
-------------
*************

This application permits to store the data sent by the developper mobile application and it helps to recognize the activity from the data sent by the user mobile application.

### Storage :

It receives the accelerometer & profile data and insert them into a Cassandra database.
Then it computes the features associated and store them into the database.

### Recognize :

It receives the accelerometer data, computes the features associated and uses the machine learning algorithm to recognize the activity performed by the user.

Deployement :
-------------
*************

The deployement of the application is automatised with docker and docker-compose.
Clone the repo and run ``docker-compose up -d``
