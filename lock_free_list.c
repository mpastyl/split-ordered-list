#include<stdio.h>
#include<stdlib.h>

/*
struct MarkedPointer{
    unsigned long long both;
}
*/

struct NodeType{
    int key;
    unsigned long long marked_next;
};


unsigned long long get_count(unsigned long long a){

    unsigned long long b = a >>63;
    return b;
}

unsigned long long get_pointer(unsigned long long a){
    unsigned long long b = a << 1;
    b= b >>1;
    return b;
}

unsigned long long set_count(unsigned long long  a, unsigned long long count){
    unsigned long long count_temp =  count << 63;
    unsigned long long b = get_pointer(a);
    b = b | count_temp;
    return b;
}

unsigned long long set_pointer(unsigned long long a, unsigned long long ptr){
    unsigned long long b = 0;
    unsigned long long c = get_count(a);
    b = set_count(b,c);
    ptr = get_pointer(ptr);
    b= b | ptr;
    return b;
}

unsigned long long set_both(unsigned long long a,unsigned long long ptr, unsigned long long count){
    a=set_pointer(a,ptr);
    a=set_count(a,count);
    return a;
}


//they must all be thread-private
unsigned long long   * prev;
unsigned long long   curr;
unsigned long long   next;


unsigned long long * Head=0;

int list_insert(unsigned long long * head, struct NodeType * node){
    
    int res;
    int temp=0;
    int key=node->key;
    while (1){
        if (list_find(&head,key)) return 0;
        node->marked_next = set_both(node->marked_next,get_pointer(curr),0);
        
        unsigned long long compare_value = set_both(compare_value,get_pointer(curr),0);
        unsigned long long new_value = set_both(new_value,(unsigned long long ) node,0);
        
        //if((*prev)!=compare_value){ if(temp==0)printf("wtf!! %lld %lld\n",*prev,compare_value);}
        temp++;
        res =__sync_bool_compare_and_swap(prev,compare_value,new_value);
        if (res){
            Head=head;
            return 1;
        }
     }
}


int list_delete(unsigned long long * head,int key){
    
    while (1){
        if (!list_find(&head,key))  return 0;
        unsigned long long compare_value = set_both(compare_value,get_pointer(next),0);
        unsigned long long new_value = set_both(new_value,get_pointer(next),1);

        if(!__sync_bool_compare_and_swap(&(((struct NodeType *)get_pointer(curr))->marked_next),compare_value,new_value)) 
            continue;


        compare_value = set_both(compare_value,get_pointer(curr),0);
        new_value = set_both(new_value,get_pointer(next),0);

        if(__sync_bool_compare_and_swap(prev,compare_value,new_value))
            free((struct NodeType *)get_pointer(curr));

        else list_find(&head,key);
        Head=head;//TODO: thats not very safe
        return 1;

    }
}


int list_find(unsigned long long ** head,int key){
    
    try_again:
        prev=(unsigned long long *)head;
        curr=set_both(curr,get_pointer(*prev),get_count(*prev));
        while (1){

            if(get_pointer(curr)==0) return 0;
            
            unsigned long long pointer=get_pointer(((struct NodeType * )get_pointer(curr))->marked_next);
            unsigned long long mark_bit = get_count(((struct NodeType * )get_pointer(curr))->marked_next);
            
            next = set_both(next,pointer,mark_bit);       
            int ckey= ((struct NodeType *)get_pointer(curr))->key;
            unsigned long long check=set_both(check,curr,0);
            if ((*prev) !=check) goto  try_again;

            if (get_count(next)==0){
                if (ckey>=key)
                    return (ckey==key);
                prev = &(((struct NodeType *)get_pointer(curr))->marked_next);   
            }

            else{
                
                unsigned long long compare_value = set_both(compare_value,curr,0);
                unsigned long long new_value = set_both(new_value,next,0);

                if (__sync_bool_compare_and_swap(prev,compare_value,new_value))
                    free((struct NodeType *)get_pointer(curr));

                else goto try_again;
            }

            curr=set_both(curr,next,get_count(next));
       }
       
}              


void print_list(unsigned long long * head){
        
        struct NodeType * curr=(struct NodeType *)get_pointer((unsigned long long)head);
        while (curr){
            printf("%d \n",curr->key);
            curr=(struct NodeType *)get_pointer(curr->marked_next);
        }
}

int main(){
    
    unsigned long long *global_head=Head;
    unsigned long long a=set_both(a,3,0);
    int i,j,res;
    
    //printf("%d\n",sizeof(unsigned long long));
    //printf("get pointer %lld get count %lld\n",get_pointer(a),get_count(a));
    //struct NodeType * dummy=(struct NodeType *)malloc(sizeof(struct NodeType));
    //dummy->key=15;
    //dummy->marked_next=0;
    //global_head=(unsigned long long *) dummy;
    struct NodeType * node = (struct  NodeType *)malloc(sizeof(struct NodeType));
    /*node->key=7;
    int res= list_insert(Head,node);

    node = (struct  NodeType *)malloc(sizeof(struct NodeType));
    node->key=8;
    res= list_insert(Head,node);
    node = (struct  NodeType *)malloc(sizeof(struct NodeType));
    node->key=2;
    res= list_insert(Head,node);
    node = (struct  NodeType *)malloc(sizeof(struct NodeType));
    node->key=4;
    res= list_insert(Head,node);
    node = (struct  NodeType *)malloc(sizeof(struct NodeType));
    node->key=4;
    res= list_insert(Head,node);
    */
    srand(time(NULL));
    //res=delete(Head,2);

    #pragma omp parallel for num_threads(8) shared(Head) private(i,j,res)
    for(i=0;i<8;i++){
        for(j=0;j<10;j++){
            node = (struct  NodeType *)malloc(sizeof(struct NodeType));
            node->key=i*10+j;
            res=list_insert(Head,node);
        }   
    }

    #pragma omp parallel for num_threads(8) shared(Head) private(i,j,res)
    for(i=0;i<8;i++){
        for(j=0;j<10;j++){
            res=list_delete(Head,i*10+j);
        }
    }
    print_list(Head);
    return 1;
}
