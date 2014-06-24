#include<stdio.h>
#include<stdlib.h>
#include<time.h>
int t[1000][1000];
int main(int argc, char * argv[]){

  if(argc!=6){
    printf("Usage: ./generator.out points clusters dimensions range range_cluster\n");
    return 1;
  }

  int n = atoi(argv[1]);
  int clusters = atoi(argv[2]);
  int dim = atoi(argv[3]);
  int range = atoi(argv[4]);
  int range_cluster = atoi(argv[5]);

  srand(time(NULL));

  for(int i = 0; i < clusters; i++){
    for(int j = 0; j < dim; j++)
      t[i][j]=rand()%range;
  }

  for(int i = 0; i < n; i++){
    printf("%d ", i);
    int k = rand()%clusters;
    for(int j = 0; j < dim; j ++)
      printf("%d ",t[k][j]+rand()%range_cluster);
    printf("\n");
  }

  return 0;
}
