#include<stdio.h>
#include<stdlib.h>

int main(int argc, char * argv[]){

  if(argc!=4){
    printf("Usage: ./generator.out nr_of_points dimensions range");
  }

  int n = atoi(argv[1]);
  int dim = atoi(argv[2]);
  int range = atoi(argv[3]);

  for(int i = 0; i < n; i++){
    printf("%d ", i);
    for(int j = 0; j < dim; j ++)
      printf("%d ",rand()%range);

    printf("\n");
  }

  return 0;
}
