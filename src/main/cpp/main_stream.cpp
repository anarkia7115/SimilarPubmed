#include <set>
#include <map>
#include <utility>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <thread>
#include "TermPwMat.hpp"

using namespace std;
std::mutex mtx;
void thread_func( const map<int, vector<pair<int, float> > > *mat, const int top_k);
void readDocLine(const map<int, vector<pair<int, float> > > *mat, map<int, float> &pwMap, int &src_pmid, string line);
void readMatLine(map<int, vector<pair<int, float> > > &mat, string line);
vector<string> calcAndSortScoreForOnePmid( const std::map<int, vector<pair<int, float> > > *mat , string line, int top_k);

struct paircomp {
    bool operator() (const pair<int, float>& lhs, const pair<int, float>& rhs) const
    {
        return lhs.second > rhs.second;
    }
};

void fixPush( set<pair<int, float> , paircomp> &pwSet , int key, float value, int top_k);

int main(int argc, char* argv[]) {

    // read args
    if (argc < 2) {
        cerr 
            << "not enough arguments!\n"
            << "./calcScores <mat_file> "
            << endl;
        return -1;
    }

    int NUM_THREADS = 1;

    // check args
    if (argc == 3) {
        NUM_THREADS = atoi(argv[2]);
        cerr << "thread number is: " << NUM_THREADS << endl;
    } else {
        cerr << "thread number not set... single thread process..." << std::endl;;
    }

    int top_k = 101;
    /*
    string dt_file = "/home/gcbi/data/docTermInRange.txt";
    string td_file = "/home/gcbi/data/termDocTable.txt";
    string out_file = "/home/gcbi/data/result_cpp_1w.csv";
    */

    // declare
    //string td_file(argv[1]);
    string termPw_archive_file(argv[1]);

    //ifstream mat_file(td_file.c_str());

    int src_pmid;
    int rel_pmid;
    float src_weight;
    float rel_weight;

    // read mat to map
    /*
    string line;
    int lnr = 0;
    while (getline(mat_file, line)) {
        if (lnr % 10000 == 0) {
            cerr << lnr << "lines read." << endl;
        }
        readMatLine(mat, line);
        lnr++;
    }
    */
    // read mat
    TermPwMat * termPw = new TermPwMat;
    termPw->loadMat(termPw_archive_file);
    std::map<int, vector<pair<int, float> > > mat = termPw->getMat();
    delete termPw;

    cerr << "mat read!" << endl;

    // TODO: move getline into thread function

    // multi thread 

    std::thread t[NUM_THREADS];

    for (int i = 0; i < NUM_THREADS; ++i) {
        t[i] = thread(thread_func, &mat, top_k);
    }

    for (int i = 0; i < NUM_THREADS; ++i) {
        t[i].join();
    }
}

void thread_func(
        const map<int, vector<pair<int, float> > > *mat
        , const int top_k) {

    // calculate score and sort result
    // mesure time
    while(true) {
      // check getline status
      mtx.lock();
      string line;
      if(!getline(std::cin, line)){
        mtx.unlock();
        break;
      }
      mtx.unlock();

      // calculation
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      vector<string> resultLines = calcAndSortScoreForOnePmid(mat, line, top_k);
      std::chrono::steady_clock::time_point end= std::chrono::steady_clock::now();

      // write to file
      //mtx.lock();
      std::cerr << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() <<std::endl;

      for (string rl: resultLines){
        std::cout << rl;
      }

      //mtx.unlock();
    }
}

vector<string> calcAndSortScoreForOnePmid(
        const std::map<int, vector<pair<int, float> > > *mat
      , string line, int top_k) {

        map<int, float> pwMap;
        int src_pmid;

        // parse doc line to map
        std::cerr << "read doc line..." << std::endl;
        readDocLine(mat, pwMap, src_pmid, line);

        // map to set pair
        set<pair<int, float>, paircomp > pwSet;

        for (auto const &pw : pwMap) {
            int rel_pmid = pw.first;
            float score = pw.second;
            fixPush(pwSet, rel_pmid, score, top_k);
            //pwSet.insert({rel_pmid, score});
        }

        vector<string> resultLines;
        string rl;
        // write top k to file
        int iter_k = 0;
        for (auto const &it : pwSet) {
            if (iter_k > top_k) {
                break;
            }
            // skip self
            if (src_pmid == it.first) {
              continue;
            }
            rl = to_string(src_pmid) + "\t" + to_string(it.first) + "\t" + to_string(it.second) + "\n";
            resultLines.push_back(rl);
            iter_k +=1;
        }
        cerr << "ready to write: " << src_pmid << "to file" << endl;
        return resultLines;

}

void readDocLine(const map<int, vector<pair<int, float> > > *mat, 
        map<int, float> &pwMap, int &src_pmid, string line) {
    stringstream linestream(line);
    string item;

    // src_pmid
    char delim1 = '\t';
    getline(linestream, item, delim1);
    src_pmid = stoi(item);

    // tw vector
    char delim2 = ',';
    while(getline(linestream, item, delim1)) {
        stringstream pairstream(item);
        string pairitem;

        // get pmid and weight
        getline(pairstream, pairitem, delim2);
        int term_id = stoi(pairitem);

        // get rel_pw
        std::vector<std::pair<int, float> > rel_pwVec;
        if (mat->find(term_id) != mat->end()){
          rel_pwVec = mat->at(term_id);
        }

        // get src_weight
        getline(pairstream, pairitem, delim2);
        float src_weight = stod(pairitem);
        //pwMap[term_id] = src_weight

        // add score
        for (auto const& pw: rel_pwVec) {
            int rel_pmid = pw.first;
            float rel_weight = pw.second;
            // if rel_pmid in result
            if (pwMap.find(rel_pmid) != pwMap.end()) {
                pwMap[rel_pmid] = src_weight * rel_weight + 
                    pwMap[rel_pmid];
            }
            else {
                pwMap[rel_pmid] =  src_weight * rel_weight;
            }
        }

    }
    //return pwMap;
}

void readMatLine(map<int, vector<pair<int, float> > > &mat, string line) {
    stringstream linestream(line);
    int term_id;
    string item;
    //string item_empty;

    // term_id
    char delim1 = '\t';
    getline(linestream, item, delim1);
    //getline(linestream, item_empty, delim1);
    term_id = stoi(item);

    // pw vector
    vector<pair<int, float> > rel_vec;
    char delim2 = ',';
    while (getline(linestream, item, delim1)) {
        /*
        if(!getline(linestream, item_empty, delim1)){
            break;
        }
        */
        stringstream pairstream(item);
        string pairitem;

        // get pmid and weight
        getline(pairstream, pairitem, delim2);
        int rel_pmid = stoi(pairitem);
        getline(pairstream, pairitem, delim2);
        float rel_weight = stod(pairitem);
        rel_vec.push_back({rel_pmid, rel_weight});
    }
    mat[term_id] = rel_vec;

}

void fixPush(
        std::set<std::pair<int, float>
          , paircomp> &pwSet
      , int key, float value, int top_k) {

    // if not full
    //std::cerr << "try pushing..." << std::endl;
    //std::cerr << "set size:" << pwSet.size() << std::endl;
    if (pwSet.size() < top_k) {
        //std::cerr << "insert" << std::endl;
        pwSet.insert({key, value});
    } else {
        //std::cerr << "current rbegin:" << pwSet.rbegin()->second << std::endl;
        if (value > pwSet.rbegin()->second){
            // replace
            pwSet.erase(--pwSet.end());
            pwSet.insert({key, value});
        }
    }
}
