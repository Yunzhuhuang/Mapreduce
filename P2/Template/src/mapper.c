#include "mapper.h"

intermediateDS *lList;
// combined value list corresponding to a word <1,1,1,1....>
valueList *createNewValueListNode(char *value){
	valueList *newNode = (valueList *)malloc (sizeof(valueList));
	strcpy(newNode -> value, value);
	newNode -> next = NULL;
	return newNode;
}

// insert new count to value list
valueList *insertNewValueToList(valueList *root, char *count){
	valueList *tempNode = root;
	if(root == NULL)
		return createNewValueListNode(count);
	while(tempNode -> next != NULL)
		tempNode = tempNode -> next;
	tempNode -> next = createNewValueListNode(count);
	return root;
}

// free value list
void freeValueList(valueList *root) {
	if(root == NULL) return;

	valueList *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		free(tempNode);
	}
}

// create <word, value list>
intermediateDS *createNewInterDSNode(char *word, char *count){
	intermediateDS *newNode = (intermediateDS *)malloc (sizeof(intermediateDS));
	strcpy(newNode -> key, word);
	newNode -> value = NULL;
	newNode -> value = insertNewValueToList(newNode -> value, count);
	newNode -> next = NULL;
	return newNode;
}

// insert or update a <word, value> to intermediate DS
intermediateDS *insertPairToInterDS(intermediateDS *root, char *word, char *count){
	intermediateDS *tempNode = root;
	if(root == NULL)
		return createNewInterDSNode(word, count);
	while(tempNode -> next != NULL) {
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value = insertNewValueToList(tempNode -> value, count);
			return root;
		}
		tempNode = tempNode -> next;

	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value = insertNewValueToList(tempNode -> value, count);
	} else {
		tempNode -> next = createNewInterDSNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeInterDS(intermediateDS *root) {
	if(root == NULL) return;

	intermediateDS *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		freeValueList(tempNode -> value);
		free(tempNode);
	}
}

// emit the <key, value> into intermediate DS
void emit(char *key, char *value) {
	//printf("Key is %s , Value is %s\n", key, value);
	lList = insertPairToInterDS(lList, key, value);
}

// map function
void map(char *chunkData){
	// you can use getWord to retrieve words from the
	// chunkData one by one. Example usage in utils.h

	// getWord usage - retrieves words from the chunk passed until it is fully traversed
	// given a chunk of data chunkData, the call to getWord should look as below:
	int i = 0;
	char *buffer;
	while ((buffer = getWord(chunkData, &i)) != NULL){
		//printf("Word found: %s\n",buffer);
		emit(buffer, "1"); //If it doesn't work do &buffer instead
	}
}
//create file name/path for fopen
void createFile(char *word, char *file){
	strcpy(file, mapOutDir);
	strcat(file, "/");
	strcat(file, word);
	strcat(file, ".txt");
}
//write word and count to file
void writeCount(valueList *root, FILE *dest, char *word){
	fprintf(dest, "%s", word);
	char count[] = " 1";
	while (root != NULL){
		root = root -> next;
		fprintf(dest, "%s", count);
	}
	fclose (dest);
}

//write intermediate data to separate word.txt files
//Each file will have only one line : word 1 1 1 1 1 ...
void writeIntermediateDS() {
	while (lList != NULL){
		//char word[20];
		//strcpy(word, lList -> key);
		char file[100];
		createFile(lList -> key, file);
	  FILE* dest = fopen (file, "w");
		if(dest == NULL){
			perror("Failed: ");
		}
		valueList *root = lList -> value;
		writeCount(root, dest, lList -> key);
		lList = lList -> next;
	}
	freeInterDS(lList);
}

int main(int argc, char *argv[]) {

	if (argc < 2) {
		printf("Less number of arguments.\n");
		printf("./mapper mapperID\n");
		exit(0);
	}
	// ###### DO NOT REMOVE ######
	mapperID = strtol(argv[1], NULL, 10);
	printf("%c", mapperID);

	// ###### DO NOT REMOVE ######
	// create folder specifically for this mapper in output/MapOut
	// mapOutDir has the path to the folder where the outputs of
	// this mapper should be stored
	mapOutDir = createMapDir(mapperID);

	// ###### DO NOT REMOVE ######
	while(1) {
		// create an array of chunkSize=1024B and intialize all
		// elements with '\0'
		char chunkData[chunkSize + 1]; // +1 for '\0'
		memset(chunkData, '\0', chunkSize + 1);

		char *retChunk = getChunkData(mapperID);
		if(retChunk == NULL) {
			break;
		}

		strcpy(chunkData, retChunk);
		free(retChunk);

		map(chunkData);
	}

	// ###### DO NOT REMOVE ######
	writeIntermediateDS();

	return 0;
}
