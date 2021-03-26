#  int arr[9] = {3,7,8,5,2,1,9,5,4};
#  for (int i =1 ;i<9;i++){
#         int temp = arr[i];
#         int  j = i - 1;
#         for(;j>=0 && arr[j] >temp;j-- ){
#             arr[j+1] = arr[j];
#         }
#         arr[j+1] = temp;
# }