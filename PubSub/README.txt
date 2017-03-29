Readme for using the Publish/Subscribe system
------------------------------------------------

1. Compile the class files using the Topic.java, Event.java, EventManager.java, and PubSubAgent.java.

2. The configuration files for the PubSubAgent will be created in the current directory of the class files when it is executed.

3. The Event Manager is started by executing: "java EventManager <serverport>". Use port 30097, for example.

4. The PubSubAgent can be started in two modes: Publisher, or Subscriber.
	- Start a subscriber by executing "java PubSubAgent subscriber <userID>". The userID is a unique identifier for a subscriber.
	- Start a publisher by executing "java PubSubAgent publisher <advertise/publish>". A new topic should ideally be advertised first, and then articles can be published.

5. Whenever a new topic is advertised by a publisher, all online subscribers are asked if they would wish to subscribe to it.

6. Each time a subscriber reconnects, their subscription preferences would be checked with the user again. They can, at that point, subscribe/un-subscribe to any possible topic.

7. The only configuration that has to be changed in the .java file is the list of available ports the Event Manager can use for sharing load. This is done on line 295 of EventManager.java. This line is marked with a "/* configurable */" comment at the end.

The config.properties file for the PubSubAgent lists the following fields:
emipaddr=localhost	-> the IP of the Event Manager is specified here
userid=0		-> unique identifier of the PubSubAgent. Kept as 0 for subscriber, and a positive integer value for subscriber.
selfport=8000		-> port where the PubSubAgent would create a Server socket at and listen for incoming TCP connections.
topic=football		-> topic to be advertised, or the topic under which the article is being published (only for publisher).
selfipaddr=localhost	-> local server IP address.
content=manutd		-> content of the article being published (only for publisher).
emport=30097		-> Server port of the Event Manager.
title=ggmu		-> title of the article being published (only for publisher).

For any further technical help, please reach out to the developer, Krishna Tippur Gururaj, at kxt2163@rit.edu.
