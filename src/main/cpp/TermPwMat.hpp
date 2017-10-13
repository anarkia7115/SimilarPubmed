#include <set>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include <map>
#include <utility>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>

class TermPwMat {

private:
  std::map<int, std::vector<std::pair<int, float> > > mat;

public:

  ~TermPwMat() {
    mat.clear();
  }

  void readMat(std::string td_file) {

    mat.clear();
    std::ifstream mat_file(td_file.c_str());

    std::string line;
    int lnr = 0;
    while (getline(mat_file, line)) {
      if (lnr % 10000 == 0) {
        std::cout << lnr << "lines read." << std::endl;
      }
      readMatLine(line);
      lnr++;
    }
  }

  std::map<int, int> loadPmids(std::string pmids_file) {
    std::ifstream ifs(pmids_file);
    std::string line;
    std::map<int, int> pmid_map;

    int col_num = 0;

    while(getline(ifs, line)) {
      int pmid = stoi(line);
      pmid_map[pmid] = col_num;
      col_num++;
    }

    return pmid_map;
  }

  void savePmids(std::string pmids_file) {
    // collect pmid
    std::set<int> pmid_set;
    int lnr = 0;
    for (auto& termPw : mat) {
      int term_id = termPw.first;
      auto pw_vec = termPw.second;

      for (auto& pw : pw_vec) {
        int pmid = pw.first;
        pmid_set.insert(pmid);
        float weight = pw.second;
      }

      if (lnr % 10000 == 0) {
        std::cerr << lnr << "mat lines read." << std::endl;
      }
      lnr += 1;
    }

    // write pmids to file
    std::ofstream ofs(pmids_file);
    for (auto& pmid : pmid_set) {
      ofs << pmid << std::endl;
    }
    ofs.close();
  }

  void printMat() {
    for (auto& termPw : mat) {
      int term_id = termPw.first;
      auto pw_vec = termPw.second;

      std::cout << term_id;
      for (auto& pw : pw_vec) {
        int pmid = pw.first;
        float weight = pw.second;
        std::cout << "\t" << pmid << "," << weight;
      }
      std::cout << std::endl;
    }
  }

  void saveMat(std::string archive_file) { 

    std::ofstream ofs(archive_file, std::ios::out | std::ios::binary);
    boost::archive::binary_oarchive oa(ofs);

    oa << mat;
    ofs.close();
  }

  void loadMat(std::string archive_file) {

    mat.clear();
    std::ifstream ifs(archive_file, std::ios::in | std::ifstream::binary);
    boost::archive::binary_iarchive ia(ifs);

    ia >> mat;
  }

  std::map<int, std::vector<std::pair<int, float> > > getMat() {
    return mat;
  }

  void readMatLine(std::string line) {
    std::stringstream linestream(line);
    int term_id;
    std::string item;
    std::string item_empty;

    // term_id
    char delim1 = '\t';
    getline(linestream, item, delim1);
    //getline(linestream, item_empty, delim1);
    term_id = stoi(item);

    // pw vector
    std::vector<std::pair<int, float> > rel_vec;
    char delim2 = ',';
    while (getline(linestream, item, delim1)) {
      /*
      if(!getline(linestream, item_empty, delim1)){
        break;
      }
      */
      std::stringstream pairstream(item);
      std::string pairitem;

      // get pmid and weight
      getline(pairstream, pairitem, delim2);
      int rel_pmid = stoi(pairitem);
      getline(pairstream, pairitem, delim2);
      float rel_weight = stod(pairitem);
      rel_vec.push_back({rel_pmid, rel_weight});
    }
    mat[term_id] = rel_vec;
  }
};
