#include<stdio.h>
#include<stdlib.h>
#include<time.h>
int main(int argc, char * argv[]){

  if(argc!=5){
    printf("Usage: ./generator.out start_point interval dimensions range\n");
    return 1;
  }

  int start_point = atoi(argv[1]);
  int interval = atoi(argv[2]);
  int dim = atoi(argv[3]);
  int range = atoi(argv[4]);
  
  srand(time(NULL)+start_point);
  for(int i = start_point; i < start_point + interval; i++){
    printf("%d ", i);
    for(int j = 0; j < dim; j ++)
      printf("%d ",rand()%range);
    printf("\n");
  }

  return 0;
}
