#include <iostream>
#include <queue>
#include <set>
#include <utility>

struct paircomp {
    bool operator() (const std::pair<int, float>& lhs, const std::pair<int, float>& rhs) const
    {
        return lhs.second > rhs.second;
    }
};
void fixPush( std::set<std::pair<int, float> , paircomp> &pwSet , int key, float value, int top_k);

int main() {
    int top_k = 5;
    std::set<std::pair<int, float> , paircomp> q;
    for (int i: {9.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0}) {
        //q.push({1, i});
        fixPush(q, 1, i, top_k);
    }
    for(auto i : q){
        std::cout << i.first << ":" << i.second << std::endl;
    }
    return 0;
}


void fixPush(
        std::set<std::pair<int, float>
          , paircomp> &pwSet
      , int key, float value, int top_k) {

    // if not full
    std::cout << "try pushing..." << std::endl;
    std::cout << "set size:" << pwSet.size() << std::endl;
    if (pwSet.size() < top_k) {
        std::cout << "insert" << std::endl;
        pwSet.insert({key, value});
    } else {
        std::cout << "current rbegin:" << pwSet.rbegin()->second << std::endl;
        if (value > pwSet.rbegin()->second){
            // replace
            pwSet.erase(--pwSet.end());
            pwSet.insert({key, value});
        }
    }
}
