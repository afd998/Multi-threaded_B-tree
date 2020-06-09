#include "btree.h"
#include <mutex>
#include <unordered_map>
std::mutex mute;
std::atomic<long long> idg;

using mutexUMap = std::unordered_map<long, std::shared_ptr<std::mutex>>;
auto locked_map = std::make_unique<mutexUMap>();

std::shared_ptr<std::mutex> acquire_node_mutex(long id)
{
	if (locked_map->find(id) == locked_map->end())
	{

		auto mut_ptr= std::make_shared<std::mutex>();
		mute.lock();
		locked_map->insert({id, mut_ptr});

		//std::cout << locked_map->size() << std::endl;
		//std::cout << "new" << std::endl;

		auto mutexx = locked_map->at(id); 
				mute.unlock();

		return mutexx;
	}
	else
	{
			//std::cout << "allready" << std::endl;
		mute.lock();

		auto mutexx = locked_map->at(id); 
				mute.unlock();

		return mutexx;
	}
}

void lock_node(long id)
{
	//	mute.lock();

	auto mut = acquire_node_mutex(id);
	mut->lock();
//	mute.unlock();

}
void unlock_node(long id)
{	
//	mute.lock();

	auto mut = acquire_node_mutex(id);
	mut->unlock();
//mute.unlock();

}


bplustree::bplustree()
{
    root = new bplustree_leaf_node();
    memset(root, 0, sizeof(struct bplustree_leaf_node));
    root->next = NULL;
		root->id = idg++;
    root->is_leaf = true;    
    root->key_count = 0;
}

void* bplustree::search(struct bplustree *t, const unsigned char *key, int key_len)
{
    LOG("Get key=" << std::string((const char *)key, key_len));
    std::stack <struct bplustree_node *> ancestors;
    struct bplustree_leaf_node *pleaf = 
	(struct bplustree_leaf_node *)LeafSearch(t, key, key_len, ancestors);
	
    void *value = NULL;
    int idx = 0;
    volatile uint64_t v = 0;
    int ret = -1;
    if(!pleaf) {
	LOG ("could not find key");
	return NULL;
    }


    int keycount = pleaf->key_count;
    for(idx = 0; idx < keycount; idx++) {
		
	unsigned char *router_key = pleaf->slots[idx].key;

	if((ret = memcmp((void *)router_key,
			 (void *)key, EMBEDEDSIZE)) >= 0) {
	    value = (void *)pleaf->slots[idx].val[0];
	    break;
	}
    }

    if(ret < 0) {
	struct bplustree_leaf_node * next = 
	    (struct bplustree_leaf_node *) pleaf->next;
	bool retry = false;
	while(next && (memcmp((void *)key, (void *)next->slots[0].key,
			      EMBEDEDSIZE) >= 0)) {
	    retry = true;
	    pleaf = next;
	    next = (struct bplustree_leaf_node *) pleaf->next;
	}
    }

    if(0 == ret) {
	LOG("value = " << (void*)value);
	LOG("   found value, slot="
	    << idx
	    << ", value="
	    << value);
	return value;
    }

    LOG("cannot find");
    return NULL;
}

bool bplustree::FindNext(const unsigned char *key, int key_len, 
			 struct bplustree_node *node,
			 struct bplustree_node **next)
{
    const int keycount = node->key_count;
    int idx = 0;
    LOG("FindNext keycount = " << keycount);
    for(idx = 0; idx < keycount; idx++) {
	unsigned char *router_key = 
	    (unsigned char *) ((struct bplustree_index_node *)node)->slots[idx].key;
		
	LOG("FindNext idx = " << idx);
	LOG("FindNext key = " << std::string((const char *)key, key_len));

	LOG("FindNext rounter_key = " << std::string((const char *)GET_KEY_STR(router_key), 
						     GET_KEY_LEN(router_key)));

	if(memcmp((void *)router_key,
		  (void *)key, EMBEDEDSIZE) >= 0) {
	    break;
	}
	
    }

    if(idx < keycount) {
	LOG("ptr = " << (void *)((struct bplustree_index_node *)node)->slots[idx].ptr);
	*next =  ((struct bplustree_index_node *)node)->slots[idx].ptr;
	return true;
    }

    struct bplustree_node * sibling = node->next;

    if(sibling && (memcmp((void *)key, 
			  (void *)((struct bplustree_index_node *)sibling)->slots[0].key,
			  EMBEDEDSIZE) >=0))
	{
	    *next = sibling;
	    return false;
	}
	

    LOG("ptr = " << (void *)((struct bplustree_index_node *)node)->slots[keycount].ptr);
    *next =  ((struct bplustree_index_node *)node)->slots[keycount].ptr;
    return true;

}

bplustree::bplustree_node* bplustree::LeafSearch(struct bplustree *t, 
						 const unsigned char *key, int key_len,
						 std::stack <struct bplustree_node *> &ancestors)

{
    struct bplustree_node* pnode = t->root;
    bool move_downwards = false;
    struct bplustree_node *next = NULL;
    if(!pnode) {
	return NULL;
    }
#ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
#endif

#ifdef USE_PREFETCH
    bplustree_node_prefetch(pnode);
#endif
    while(!pnode->is_leaf) {
		
	LOG("searchleaf access " << pnode);
	move_downwards = FindNext(key, key_len, pnode, &next);
	assert(next);
	if(move_downwards)
	    ancestors.push(pnode);
	pnode = next;
#ifdef USE_PREFETCH
	bplustree_node_prefetch(pnode);
#endif
	assert(pnode);
    }

#ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
    search_time += duration;
#endif
    return pnode;
}

int bplustree::insert(struct bplustree *t, const unsigned char *key, 
		      int key_len, const void *value)
{
	  //std::cout << "insert "  << std::endl;

    LOG("Put key=" << std::string((const char *)key, key_len));
    std::stack <struct bplustree_node *> ancestors;
    bool need_split = false;
    const unsigned char* split_key;
    struct bplustree_leaf_node *p_new_leaf = NULL;
    struct bplustree_leaf_node *p_leaf = 
	(struct bplustree_leaf_node *)LeafSearch(t, key, key_len, ancestors);

    if(!p_leaf) {	
	assert(false);
	return 0;
    }

#ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
#endif
    struct bplustree_leaf_node * next =
	(struct bplustree_leaf_node *) p_leaf->next;
	//mut.lock();
		lock_node(p_leaf->id);
			//mut.unlock();

	//	std::cout << "lock da root "  << std::endl;

    while(next && (memcmp((void *)key, 
			  (void *)next->slots[0].key, EMBEDEDSIZE) >= 0)) {	
	LOG("leaf = " << (void *)p_leaf);
	LOG("leaf->next = " << (void *)p_leaf->next);
	// lock what is the correct leaf
		lock_node(next->id);
	//unlock old leaf
		unlock_node(p_leaf->id);


	p_leaf = next;
	next = (struct bplustree_leaf_node *) p_leaf->next;
	LOG("Leaf update next " << (void *)next);
    }

    int idx = 0;
    bool exist = false;

    int ret;
    int keycount = p_leaf->key_count;
    for(idx = 0; idx < keycount; idx++) {
	unsigned char * router_key = p_leaf->slots[idx].key;
	

	if((ret = memcmp((void *)router_key, 
			 (void *)key, EMBEDEDSIZE)) >= 0) {
	    if(ret == 0)
		exist = true;
	    break;
	}
    }

    if(exist) {
	p_leaf->slots[idx].val[0] = (uint64_t) value;
#ifdef INDEX_STATS
	std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
	update_time += duration;
#endif
		//mut.lock();

		unlock_node(p_leaf->id);
		//mut.unlock();

	return 0;
    }
    LOG("Insert at Slot = " << idx);

    // insert + split 
    for(int i = keycount - 1;  i>= idx; i--) {
	memcpy(&p_leaf->slots[i + 1], &p_leaf->slots[i], 
	       sizeof(struct bplustree_kvslot));
    }

    memcpy((void *)p_leaf->slots[idx].key, 
	   key, EMBEDEDSIZE);

    p_leaf->slots[idx].val[0] = (uint64_t)value;
    p_leaf->key_count += 1;

    if(p_leaf->key_count > BPLUSTREE_LEAF_NODE_KEYS) {
#ifdef INDEX_STATS
	split_nums++;
#endif
	LOG("split, keycount = " << p_leaf->key_count);

	need_split = true;

	// Now split
	p_new_leaf = (struct bplustree_leaf_node *)
	    malloc(sizeof(struct bplustree_leaf_node));
	memset(p_new_leaf, 0, sizeof(struct bplustree_leaf_node));
	p_new_leaf->is_leaf = true;
	p_new_leaf->id = idg++;

	//init new leaf

	split_key = p_leaf->slots[BPLUSTREE_LEAF_MIDPOINT].key;
		
	LOG("split_key = " << std::string((const char *)GET_KEY_STR(split_key), 
					  GET_KEY_LEN(split_key)));

	keycount = p_leaf->key_count;

	for(int i = BPLUSTREE_LEAF_UPPER; i < keycount; i++) {
	    memcpy(&p_new_leaf->slots[i - BPLUSTREE_LEAF_UPPER],
		   &p_leaf->slots[i],
		   sizeof(struct bplustree_kvslot));
	}	

	p_new_leaf->key_count = keycount - BPLUSTREE_LEAF_MIDPOINT - 1; 

	p_leaf->key_count = BPLUSTREE_LEAF_MIDPOINT + 1;

	LOG("new_leaf keycount " << p_new_leaf->key_count);
	LOG("old_leaf keycount " << p_leaf->key_count);
		
	p_new_leaf->next = (struct bplustree_node *)p_leaf->next; 
	
	p_leaf->next = (struct bplustree_node *)p_new_leaf;

	if((struct bplustree_node *)p_leaf == t->root) {
	    assert(ancestors.empty());

	    struct bplustree_index_node *p_root;
	    p_root = (struct bplustree_index_node *) 
		malloc(sizeof(struct bplustree_index_node));
			
	    memset(p_root, 0, sizeof(struct bplustree_index_node));
	    memcpy((void*)p_root->slots[0].key, 
		   GET_KEY_STR(split_key),
		   GET_KEY_LEN(split_key));

	    p_root->slots[0].ptr = (struct bplustree_node *) p_leaf;
	    p_root->slots[1].ptr = (struct bplustree_node *) p_new_leaf;
	    p_root->key_count = 1;
	    p_root->is_leaf = false;
			p_root->id = idg++;

	    t->root = (struct bplustree_node *)p_root;
	}
    }
    if(need_split) {
	InsertInternal(t, (const unsigned char *)GET_KEY_STR(split_key),
		       GET_KEY_LEN(split_key),
		       (struct bplustree_node *)p_new_leaf, (struct bplustree_node *)p_leaf,
		       ancestors);
    }else{
					//mut.lock();

		unlock_node(p_leaf->id);
		 		//mut.unlock();

		}

#ifdef INDEX_STATS
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
    update_time += duration;
#endif

    return 1;
}


void bplustree::InsertInternal(struct bplustree *t, const unsigned char *key, int key_len,
			       struct bplustree_node *right, struct bplustree_node *old_node, 
			       std::stack <struct bplustree_node *> ancestors)
{
  //std::cout << "insert internal"  << std::endl;
    if(ancestors.empty()) {
	//root split
	//assert(left.oid.off == D_RW(bplustree)->root.oid.off);
			//mut.lock();
		unlock_node(old_node->id);
			//mut.unlock();

	return;	
    }

    struct bplustree_index_node *p_parent = 
	(struct bplustree_index_node *)ancestors.top();
    ancestors.pop();

    struct bplustree_index_node * next =
	(struct bplustree_index_node *)p_parent->next;
			//mut.lock();
		lock_node(p_parent->id);
				//mut.unlock();

    while(next && (memcmp((void *)key, 
			  (void *)next->slots[0].key, EMBEDEDSIZE) >= 0)) {
      				//mut.lock();
		lock_node(next->id);
		unlock_node(p_parent->id);
												//mut.unlock();


	p_parent = next;
	next = (struct bplustree_index_node*) p_parent->next;
    }
									//mut.lock();
		unlock_node(old_node->id);
							//mut.unlock();

    int keycount = p_parent->key_count;
    int idx = 0;
    for(idx = 0; idx < keycount; idx++) {
	const unsigned char *router_key = p_parent->slots[idx].key;
	if(memcmp((void *)router_key, (void *)key, EMBEDEDSIZE) >= 0) {
	    break;
	}
    }

    for(int i = keycount;  i>= idx; i--) {
	memcpy(&p_parent->slots[i + 1], &p_parent->slots[i],
	       sizeof(struct bplustree_kpslot));
    }

    memcpy((void*)p_parent->slots[idx].key, 
	   key, EMBEDEDSIZE);

    p_parent->slots[idx+1].ptr = right;
	
    p_parent->key_count += 1;

    if(p_parent->key_count <= BPLUSTREE_NODE_KEYS) {
										//mut.lock();
		unlock_node(p_parent->id);
									//mut.unlock();
	return;
    }

    LOG("internal split, keycount = " << p_parent->key_count);
	
    struct bplustree_index_node *p_new_parent;
    p_new_parent = (struct bplustree_index_node *)
	malloc(sizeof(struct bplustree_index_node));
    memset(p_new_parent, 0, sizeof(struct bplustree_index_node));

    p_new_parent->is_leaf = false;
		p_new_parent->id = idg++;

    unsigned char *split_key = p_parent->slots[BPLUSTREE_NODE_MIDPOINT].key;
    keycount = p_parent->key_count;
	
    LOG("split_key = " << std::string((const char *)GET_KEY_STR(split_key), 
				      GET_KEY_LEN(split_key)));

    for(int i = BPLUSTREE_NODE_UPPER; i <= keycount; i++) {
	memcpy(&p_new_parent->slots[i - BPLUSTREE_NODE_UPPER],
	       &p_parent->slots[i],
	       sizeof(struct bplustree_kpslot));
    }

    p_new_parent->key_count = keycount - BPLUSTREE_NODE_MIDPOINT - 1; 
    p_new_parent->next = (struct bplustree_node *)p_parent->next; 
	
    //update old parent
    p_parent->next = (struct bplustree_node *)p_new_parent;

    p_parent->key_count = BPLUSTREE_NODE_MIDPOINT;
	
    LOG("new_parent keycount " << p_new_parent->key_count);
    LOG("old_parent keycount " << p_parent->key_count);
    if((struct bplustree_node *) p_parent == t->root) {
	assert(ancestors.empty());
	struct bplustree_index_node *p_root;
	p_root = (struct bplustree_index_node *)
	    malloc(sizeof(struct bplustree_index_node));
	memset(p_root, 0, sizeof(struct bplustree_index_node));
	memcpy((void*)p_root->slots[0].key, 
	       GET_KEY_STR(split_key),
	       GET_KEY_LEN(split_key));

	p_root->slots[0].ptr = (struct bplustree_node *) p_parent;
	p_root->slots[1].ptr = (struct bplustree_node *) p_new_parent;
	p_root->key_count = 1;
	p_root->is_leaf = false;
	p_root->id = idg++;

	t->root = (struct bplustree_node *)p_root;
    }

    InsertInternal(t, (const unsigned char *) GET_KEY_STR(split_key),
		   GET_KEY_LEN(split_key),	
		   (struct bplustree_node *)p_new_parent, (struct bplustree_node *) p_parent, 

		   ancestors);

    return;

}

int bplustree::remove(struct bplustree *t, const unsigned char *key, int key_len)
{
    return 0;
}