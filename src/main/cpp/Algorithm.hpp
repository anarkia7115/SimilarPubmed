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

using namespace std;

struct paircomp {
    bool operator() (const pair<int, float>& lhs, const pair<int, float>& rhs) const
    {
        return lhs.second > rhs.second;
    }
};

class Algorithm {

  std::mutex mtx;
  string docTermFile;
  string termDocFile;
  string outFile;
  ifstream doc_file;
  ifstream mat_file;
  ofstream result_file;


  int src_pmid;
  int rel_pmid;
  float src_weight;
  float rel_weight;

  map<int, vector<pair<int, float> > > mat;

  vector<string> docLines;

  void runCalculation() {
    int docNumPerThread = this->docLines.size() / this->NUM_THREADS;
    //int lastDocNumPerThread = docLines.size() - docNumPerThread * (NUM_THREADS - 1);

    std::thread t[this->NUM_THREADS];
    auto iter = this->docLines.begin();

    for (int i = 0; i < this->NUM_THREADS; ++i) {
        if (i != this->NUM_THREADS - 1) {
            vector<string> subDocLines(iter, iter + docNumPerThread);
            iter = iter + docNumPerThread;
            t[i] = thread(thread_func, std::ref(this->result_file), &this->mat, subDocLines, top_k);
        } else {
            vector<string> subDocLines(iter, this->docLines.end());
            t[i] = thread(thread_func, std::ref(this->result_file), &this->mat, subDocLines, top_k);
        }
    }

    for (int i = 0; i < this->NUM_THREADS; ++i) {
        t[i].join();
    }

    this->closeResultFile();
  }

  void closeResultFile() {
    this->result_file.close();
  }

  void readDoc() {
    while (getline(this->doc_file, line)) {
        this->docLines.push_back(line);
    }
  }

  void readMat() {
    string line;
    int lnr = 0;
    while (getline(this->mat_file, line)) {
        if (lnr % 10000 == 0) {
            cout << lnr << "lines read." << endl;
        }
        readMatLine(this->mat, line);
        lnr++;
    }
  }

  /*
  void ~Algorithm() {
    this->closeResultFile();
  }
  */

  void Algorithm(string dt_file, string td_file, string out_file) {
    this->docTermFile = dt_file ;
    this->termDocFile = td_file ;
    this->outFile = out_file ;
    this->doc_file.open(dt_file.c_str());
    this->mat_file.open(td_file.c_str());
    this->result_file.open(out_file.c_str());
  }

  void thread_func(
          ofstream& result_file
          , const map<int, vector<pair<int, float> > > *mat
          , const vector<string> docLines , const int top_k) {
      for(string dl : docLines) {
          thread_inner_func(result_file, mat, dl, top_k);
      }
  }

  void thread_inner_func(
          ofstream& result_file
          , const map<int, vector<pair<int, float> > > *mat
          , const string line, const int top_k) {

      // calculate score and sort result
      // mesure time
      std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
      vector<string> resultLines = calcAndSortScoreForOnePmid(mat, line, top_k);
      std::chrono::steady_clock::time_point end= std::chrono::steady_clock::now();

      // write to file
      this->mtx.lock();
      std::cout << "Time difference = " << std::chrono::duration_cast<std::chrono::microseconds>(end - begin).count() <<std::endl;

      for (string rl: resultLines){
          result_file << rl;
      }
      result_file.flush();

      this->mtx.unlock();
  }

  vector<string> calcAndSortScoreForOnePmid(
          const std::map<int, vector<pair<int, float> > > *mat
        , string line, int top_k) {

          map<int, float> pwMap;
          int src_pmid;

          // parse doc line to map
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
              rl = to_string(src_pmid) + "\t" + to_string(it.first) + "\t" + to_string(it.second) + "\n";
              resultLines.push_back(rl);
              iter_k +=1;
          }
          //result_file.flush();
          cout << "ready to write: " << src_pmid << "to file" << endl;
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
          std::vector<std::pair<int, float> > rel_pwVec = mat->at(term_id);

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
      string item_empty;

      // term_id
      char delim1 = '\t';
      getline(linestream, item, delim1);
      getline(linestream, item_empty, delim1);
      term_id = stoi(item);

      // pw vector
      vector<pair<int, float> > rel_vec;
      char delim2 = ',';
      while (getline(linestream, item, delim1)) {
          if(!getline(linestream, item_empty, delim1)){
              break;
          }
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
      //std::cout << "try pushing..." << std::endl;
      //std::cout << "set size:" << pwSet.size() << std::endl;
      if (pwSet.size() < top_k) {
          //std::cout << "insert" << std::endl;
          pwSet.insert({key, value});
      } else {
          //std::cout << "current rbegin:" << pwSet.rbegin()->second << std::endl;
          if (value > pwSet.rbegin()->second){
              // replace
              pwSet.erase(--pwSet.end());
              pwSet.insert({key, value});
          }
      }
  }
};
