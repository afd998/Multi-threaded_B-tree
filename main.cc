#include <stdio.h>
#include <vector>
#include "btree.h"
#include <thread>

uint32_t ELEM = 10000000;
uint32_t THREADS = 32;
uint32_t TOTAL_OPS = 10000000;
uint32_t READ_PCT = 80;

void do_work (bplustree *tree, uint32_t OPS, uint32_t tid) {
  uint32_t local_tid = tid;

  for (uint64_t i = 0; i < OPS; i++) {
      uint64_t key = rand_r(&local_tid) % ELEM;
	
      uint32_t type = rand_r(&local_tid) % 100;
      if (type <= READ_PCT) {
	  auto value = (uint64_t)tree->search(tree, (unsigned char *)&key, 8);
	  if (value != key && value != 0) {
	      std::cout << "Error: value: " << value << " != key: " << key << std::endl;
	  }
      }
      else {
	  //During execution only insert odd keys
	  key |= 1;
	  tree->insert(tree, (unsigned char *)&key, 8, (void *)key);
      }
  }
}

int main()
{
    srand(0);
    std::vector<std::thread> myThreads(THREADS);
    for (uint32_t t = 1; t <= THREADS; t *= 2) {
	//On each thread iteration re-initialize tree, to avoid all keys being present already
	bplustree *tree = new bplustree();
	//Initialize tree with even keys
	std::vector<uint64_t> keys;
	for (uint64_t i = 0; i < ELEM; i += 2) {
	    keys.push_back(i);
	}
	std::random_shuffle(keys.begin(), keys.end());
	for (uint64_t i = 0; i < keys.size(); i++) {
	    auto ret = tree->insert(tree, (unsigned char *)&keys[i], 8, (void *)keys[i]);
	}

	uint32_t OPS = TOTAL_OPS / t;
	std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
	for (uint32_t tt = 0; tt < t; tt++) {
	    myThreads[tt] = std::thread(do_work, tree, OPS, tt);
	}
	for (uint32_t tt = 0; tt < t; tt++) {
	    myThreads[tt].join();
	}
	std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
	std::cout << "Threads: " << t << " Duration: " << duration << std::endl;
    }
    return 1;
}
