class PmidTwMat {

private:
  //std::map<int, float> pwMap;
  std::ifstream pt_ifs;

public:

  PmidTwMat(std::string input_path) {
    std::string pt_file = input_path;
    pt_ifs.open(pt_file);
  }

  int get_one(
      std::vector<std::pair<int, float> > &tw_vec
      ) {
    std::string line;
    if (getline(pt_ifs, line)) {
      pt_ifs.close();
      return -1;
    } else {
      return readDocLine(line, tw_vec);
    }
  }

  int readDocLine(
      std::string line, 
      std::vector<std::pair<int, float> > &tw_vec
      ) {
    if (tw_vec.size() != 0) {
      std::cerr << "non-empty vec: tw_vec has been cleared!" << std::endl;
      tw_vec.clear();
    }
    std::stringstream linestream(line);
    std::string item;

    // src_pmid
    char delim1 = '\t';
    getline(linestream, item, delim1);
    int src_pmid = stoi(item);

    // tw vector
    char delim2 = ',';
    while(getline(linestream, item, delim1)) {
      std::stringstream pairstream(item);
      std::string pairitem;

      // get pmid and weight
      getline(pairstream, pairitem, delim2);
      int term_id = stoi(pairitem);

      // get weight
      getline(pairstream, pairitem, delim2);
      float weight = stod(pairitem);
      tw_vec.push_back({term_id, weight});
    }

    return src_pmid;
  }
};
