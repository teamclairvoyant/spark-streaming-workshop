The Utility Producer-0.0.1-SNAPSHOT.jar can be used to
auto generate a defined set of personIds at a frequency of
every 5 seconds.

Usage: java -jar Producer-0.0.1-SNAPSHOT.jar {topicname} {pathoffile}

Where
topicname => the topic to which the generated personIds should go to.
pathoffile => the file that contains the integer that defines how many person IDs should be generated.

Ex: java -jar Producer-0.0.1-SNAPSHOT.jar myTopicName increaseImmigrantsBy.txt