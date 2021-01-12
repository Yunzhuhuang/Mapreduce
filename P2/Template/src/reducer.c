#include "reducer.h"

//the root fot the word tree
finalKeyValueDS *root = NULL;

// create a key value node
finalKeyValueDS *createFinalKeyValueNode(char *word, int count){
	finalKeyValueDS *newNode = (finalKeyValueDS *)malloc (sizeof(finalKeyValueDS));
	strcpy(newNode -> key, word);
	newNode -> value = count;
	newNode -> next = NULL;
	return newNode;
}

// insert or update an key value
finalKeyValueDS *insertNewKeyValue(finalKeyValueDS *root, char *word, int count){
	finalKeyValueDS *tempNode = root;
	if(root == NULL)
		return createFinalKeyValueNode(word, count);
	while(tempNode -> next != NULL){
		if(strcmp(tempNode -> key, word) == 0){
			tempNode -> value += count;
			return root;
		}
		tempNode = tempNode -> next;
	}
	if(strcmp(tempNode -> key, word) == 0){
		tempNode -> value += count;
	} else{
		tempNode -> next = createFinalKeyValueNode(word, count);
	}
	return root;
}

// free the DS after usage. Call this once you are done with the writing of DS into file
void freeFinalDS(finalKeyValueDS *root) {
	if(root == NULL) return;

	finalKeyValueDS *tempNode = NULL;
	while (root != NULL){
		tempNode = root;
		root = root -> next;
		free(tempNode);
	}
}

// reduce function
void reduce(char *key) {
  FILE *f;
  f = fopen(key, "r");
  if(errno == -1) {
    printf("failed to open file\n");
    return;
  }
  char word[50];
  fscanf(f, "%s", word);
  int count = 0;
  char ch;
  ch = fgetc(f);
  while(ch != EOF) {
   if(ch != ' '){
   count ++;
 }
  ch = fgetc(f);
  }
  if(ferror(f)!=0)
     printf("failed to read from the file.\n");
  root = insertNewKeyValue(root, word, count);
  fclose(f);
}

// write the contents of the final intermediate structure
// to output/ReduceOut/Reduce_reducerID.txt
void writeFinalDS(int reducerID){
  char str1[100] = "output/ReduceOut/Reduce_";
  char str2[] = ".txt";
  char reduceID[33];
  sprintf(reduceID, "%d", reducerID);
  char *reducestring = strcat(str1, strcat(reduceID, str2));
  FILE *fp;
  fp = fopen(reducestring, "w");
  if(errno == -1) {
    printf("failed to create file\n");
    return;
  }
  finalKeyValueDS *temp = root;
  while(temp!=NULL) {
    fprintf(fp, "%s %d\n", temp->key, temp->value);
    temp = temp->next;
  }
  freeFinalDS(root);
  fclose(fp);
}

int main(int argc, char *argv[]) {

	if(argc < 2){
		printf("Less number of arguments.\n");
		printf("./reducer reducerID");
	}
	// ###### DO NOT REMOVE ######
	// initialize
	int reducerID = strtol(argv[1], NULL, 10);

	// ###### DO NOT REMOVE ######
	// master will continuously send the word.txt files
	// alloted to the reducer
	char key[MAXKEYSZ];
	while(getInterData(key, reducerID))
		reduce(key);

	// You may write this logic. You can somehow store the
	// <key, value> count and write to Reduce_reducerID.txt file
	// So you may delete this function and add your logic
	writeFinalDS(reducerID);

	return 0;
}
