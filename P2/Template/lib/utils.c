#include "utils.h"

char *getChunkData(int mapperID) {
   // open existing queue
   key_t Getkey = ftok(".", MAPKEY);
   int getChunkmsgid;
   if ((getChunkmsgid = msgget(Getkey, PERM|IPC_CREAT))<0) {
     printf("msgget error\n");
     exit(-1);
    }
   // receive message
   struct msgBuffer getChunkmessage;
   if(msgrcv(getChunkmsgid, &getChunkmessage, sizeof(getChunkmessage.msgText), mapperID, 0) < 0){
	printf("error receiving in getChunkData \n");
	exit(-1);   
   }
   char *chunkData = (char *)malloc(sizeof(char) * chunkSize);
   memset(chunkData, '\0', chunkSize);
   strcpy(chunkData, getChunkmessage.msgText);
   // Check for END message and send ACK
   if (strcmp(chunkData, "END") == 0) {
   	return NULL;    
   }
   else {
   	return chunkData;
   }
}

// sends chunks of size 1024 to the mappers in RR fashion
void sendChunkData(char *inputFile, int nMappers) {
    struct msgBuffer msg; 
    memset(msg.msgText, '\0', chunkSize);

    key_t key;
    int msgid;
    // creat a unique key
    key = ftok(".", MAPKEY);

    if((msgid = msgget(key, PERM | IPC_CREAT)) < 0) {
	printf("msgget error\n");
	exit(-1);
    }

    char *word = (char *)malloc(sizeof(char) * chunkSize);
    char *buffer = (char *)malloc(sizeof(char) * chunkSize);
    char *finalBuffer = (char *)malloc(sizeof(char) * chunkSize);
    memset(buffer, '\0', chunkSize);
    memset(word, '\0', chunkSize);
    memset(finalBuffer, '\0', chunkSize);
    size_t bytesRead = 0;
    FILE* fp;
    fp = fopen (inputFile, "r");
    if(errno == -1) {
      printf("failed to open file\n");
      return;
    }
    int i;
    int j;
    int n = 1;
    char ch;
    ch = fgetc(fp);
    //read in 1 char at time making a word, if word fits in 1024 chunk then add to chunk until can't fit and then send message 
    while(ch != EOF){
	if (ch == '\n' || ch == ' ' || !validChar(ch) || ch == 0x0){
	    if(strlen(finalBuffer) + strlen(word) <= chunkSize){
		strcat(finalBuffer, word);
		memset(word, '\0', chunkSize);
		j = 0;
		word[j] = ch;
		j++;
		ch = fgetc(fp);
	     }else{
		if(n > nMappers){
		    n = 1;
		}
		strcpy(msg.msgText, finalBuffer);
		msg.msgType = n;
		if ((msgsnd(msgid, &msg, sizeof(msg.msgText), 0)) < 0) {
            	    printf("msgsnd error\n");
            	    exit(-1);
        	}
		
		n++;
		word[j] = ch;
		j++;
		memset(finalBuffer, '\0', chunkSize);
		ch = fgetc(fp);
	    }
	}else{
	    word[j] = ch;
	    j++;
	    ch = fgetc(fp);
	}
    }
    if(strlen(finalBuffer) > 0){
		strcat(finalBuffer, word);
		if(n > nMappers){
			n = 1;
		}
		strcpy(msg.msgText, finalBuffer);
		msg.msgType = n;
		if ((msgsnd(msgid, &msg, sizeof(msg.msgText), 0)) < 0) {
                   printf("msgsnd error\n");
                   exit(-1);
                }
    } 
    //let mapper know its done getting chunks of data 
    for (int i = 0; i < nMappers; i++) {
            msg.msgType = i + 1;
            memset(msg.msgText, '\0', chunkSize);
            sprintf(msg.msgText, "END");
            // send message to other child processes
            if ((msgsnd(msgid, &msg, sizeof(msg.msgText), 0)) < 0) {
                printf("msgsnd error\n");
                exit(-1);
            }
    }
}
// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers);
}

int getInterData(char *key, int reducerID) {
    int msgid;
    
    struct msgBuffer msg;
    
    //generate unique key
    key_t que_key = ftok(".", REDUCEKEY);
    //creates a message queue
    if ((msgid = msgget(que_key, PERM|IPC_CREAT))<0) {
     printf("msgget error\n");
     return -1;
    }
    if(msgrcv(msgid, (void *) &msg, sizeof(msg.msgText), reducerID, 0) < 0){
     printf("msgreceive error");
     return -1;
    }
    strcpy(key, msg.msgText);
     
   if (strcmp(key, "END") == 0)
      return 0;
    else 
      return 1;
}

void shuffle(int nMappers, int nReducers)
{
	struct msgBuffer msg;
	memset(msg.msgText, '\0', chunkSize);

	key_t key;
	int msgid;

	key = ftok(".", REDUCEKEY);

	if ((msgid = msgget(key, PERM | IPC_CREAT)) < 0)
	{
		printf("msgget error\n");
		exit(-1);
	}
	
	for (int i = 0; i < nMappers; i++)
	{
		int pathlength = 50;
		//create path
		char *path = malloc(pathlength * sizeof(char));;
		strcpy(path, "output/MapOut/Map_");
		char fileNum[4];
		sprintf(fileNum, "%d", i + 1);

		strcat(path, fileNum);
		DIR *dir = opendir(path);
		if (dir == NULL)
		{
			printf("The path passed is invalid");
			exit(-1);
		}
		struct dirent *entry;
		char *path2 = malloc(pathlength * sizeof(char));
		memset(path2, '\0', pathlength);
		while ((entry = readdir(dir)) != NULL)
		{
			strcpy(path2, path);
			if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			{
				continue;
			}
			else
			{
				int reducerId = hashFunction(entry->d_name, nReducers) + 1;
				//make path for each file and send path to reducer
				strcat(path2, "/");
				strcat(path2, entry->d_name);

				msg.msgType = reducerId;
				strcpy(msg.msgText, path2);
				
				if ((msgsnd(msgid, (void *)&msg, sizeof(msg.msgText), 0)) < 0)
				{
					printf("msgsnd error\n");
					exit(-1);
				}
				
			}
			memset(path2, '\0', pathlength);
		}
		closedir(dir);

	}
	for(int i = 0; i < nReducers; i++){
		msg.msgType = i + 1;
		memset(msg.msgText, '\0', chunkSize);
		sprintf(msg.msgText, "END");
		if ((msgsnd(msgid, &msg, sizeof(msg.msgText), 0)) < 0)
		{
			printf("msgsnd error\n");
			exit(-1);
		}
	}
}

// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		// read a single word at a time from chunk
		// printf("%d\n", i);
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}
