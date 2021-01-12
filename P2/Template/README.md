The purpose of my program is to be a backbone for a mapreduce function which gets the word count for a given file and output the count to a text file. The functions written are utility funcitons used in the mapreduce, mapper, and reducer source files.

The code can be compiled using the Makefile in the Template folder.

There are four functions. The first two are sendChunkData and getChunkData which are called by mapper. sendChunkData first opens a message queue which is connected to getChunkData. It then opens a file and sends chunks of 1024 bytes of characters to getChunkData through the message queue. When sendChunkData finds that there are no more characters to send from the file it sends an "END" message and closes the message queue. getChunkData connects to the message queue with sendChunkData and returns the recieved chunks to mapper. When getChunkData recieves and "END" message it returns NULL and mapper moves on.
The third function is shuffle, called once mapper is complete. shuffle first opens a new message queue, it then traverses the directory made by mapper and sends the file paths of the word text files to various reducers via the message queue. When shuffle finds that it has traversed all of the directory it sends an "END" message to the reducers and closes the message queue. The fourth and final function is getInterData which is called by the reducers. getInterData connects to the message queue created by shuffle and recieves the file paths of the word text files. getInterData returns a 1 for each file path which is then reduced by reducer. When getInterData recieves an "END" message it returns 0 and reducer moves on.

Project Group: 25

Members:
Collin Campbell, camp0609
Clara Huang, huan2089
Hunter Bahl, bahlx038

Credits:
sendChunkData: all contributed, Collin started
getChunkData: all contributed, Hunter started
shuffle: all contributed, Collin started
getInterData: all contributed, Clara started
